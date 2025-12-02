use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use lattice_db::{GraphBuilder, LatticeDb, QueryBuilder};

// test lookup time of nodes all connected to one node
fn bench_supernode(c: &mut Criterion) {
    let mut group = c.benchmark_group("Query");
    group.sample_size(100);

    let sizes: Vec<_> = (1..5).map(|v| 10usize.pow(v)).collect();
    for size in sizes.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("Supernode", size), size, |b, &size| {
            let (db, _) = LatticeDb::create_temporary().unwrap();
            let mut wr = db.begin_write().unwrap();
            let prop_follows = wr.register_property(None, &()).unwrap();
            let prop_type = wr.register_property(None, &()).unwrap();

            // populate graph
            let mut graph = GraphBuilder::new();
            let celebrity = graph
                .new_vertex()
                .new_attribute(prop_type, "celebrity")
                .unwrap()
                .handle();
            for _ in 0..size {
                let fan = graph.new_vertex().handle();
                graph.new_edge(fan, prop_follows, celebrity).unwrap();
            }

            wr.save_graphs_parallel(vec![graph]).unwrap();
            wr.commit().unwrap();

            // query for who follows the celebrity
            let mut query = QueryBuilder::new();
            let celeb_node = query.match_attr(prop_type, "celebrity").unwrap();
            let followers = query.match_incoming(prop_follows, celeb_node).unwrap();
            query.set_root(followers);
            let query = query.compile().unwrap();

            let reader = db.begin_read().unwrap();
            b.iter(|| {
                let res = reader.search(black_box(&query)).unwrap();
                assert_eq!(res.len(), size);
            });
        });
    }
}

// intersections perform searches from the smaller set to resolve instantly
fn bench_intersection(c: &mut Criterion) {
    let mut group = c.benchmark_group("Query");

    let big_size = 100_000;
    let small_size = 5;

    group.bench_function("Intersection", |b| {
        let (db, _) = LatticeDb::create_temporary().unwrap();
        let mut wr = db.begin_write().unwrap();

        let p_big_a = wr.register_property("big a", &()).unwrap();
        let p_big_b = wr.register_property("big b", &()).unwrap();
        let p_small = wr.register_property("small", &()).unwrap();

        // give attributes out so that many vertices have "big a" and "big b", while only a few have "small"
        let mut graph = GraphBuilder::new();
        for i in 0..big_size {
            let mut v = graph.new_vertex();
            v.new_attribute(p_big_a, 1u8).unwrap();
            v.new_attribute(p_big_b, 1u8).unwrap();
            if i < small_size {
                v.new_attribute(p_small, 1u8).unwrap();
            }
        }

        wr.save_graphs_parallel(vec![graph]).unwrap();
        wr.commit().unwrap();

        // query for vertices with all attrs
        let mut query = QueryBuilder::new();
        let n_a = query.match_attr(p_big_a, 1u8).unwrap();
        let n_b = query.match_attr(p_big_b, 1u8).unwrap();
        let n_c = query.match_attr(p_small, 1u8).unwrap();
        let root = query.group_and(vec![n_a, n_b, n_c]).unwrap();
        query.set_root(root);
        let query = query.compile().unwrap();

        let reader = db.begin_read().unwrap();
        b.iter(|| {
            let res = reader.search(black_box(&query)).unwrap();
            assert_eq!(res.len(), small_size);
        });
    });
}

// test lookup time for a large set of different properties
fn bench_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("Query");
    group.sample_size(100);

    let sizes: Vec<_> = (1..5).map(|v| 10usize.pow(v)).collect();
    for size in sizes.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("Union", size), size, |b, &size| {
            let (db, _) = LatticeDb::create_temporary().unwrap();
            let mut wr = db.begin_write().unwrap();

            // graph with many vertices, each with a unique attr
            let mut graph = GraphBuilder::new();
            let mut props = vec![];
            for _ in 0..size {
                props.push(wr.register_property(None, &()).unwrap());
            }
            for p in &props {
                graph.new_vertex().new_attribute(*p, 1u8).unwrap();
            }
            wr.save_graphs_parallel(vec![graph]).unwrap();
            wr.commit().unwrap();

            // query for every attr at once
            let mut query = QueryBuilder::new();
            let mut terms = vec![];
            for p in &props {
                terms.push(query.match_attr(*p, 1u8).unwrap());
            }
            let root = query.group_or(terms).unwrap();
            query.set_root(root);
            let query = query.compile().unwrap();

            let reader = db.begin_read().unwrap();
            b.iter(|| {
                let res = reader.search(black_box(&query)).unwrap();
                assert_eq!(res.len(), size);
            });
        });
    }
}

// lookup a node based on its linking with another node's X amount of links
fn bench_query_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("Query");
    group.sample_size(100);

    let sizes: Vec<_> = (1..5).map(|v| 10usize.pow(v)).collect();
    for size in sizes.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("Deep Chain", size), size, |b, &size| {
            let (db, _) = LatticeDb::create_temporary().unwrap();
            let mut wr = db.begin_write().unwrap();
            let mut graph = GraphBuilder::new();
            let vtxs: Vec<_> = (0..size).map(|_| graph.new_vertex().handle()).collect();
            let props: Vec<_> = (0..size - 1)
                .map(|_| wr.register_property(None, &()).unwrap())
                .collect();
            for i in 0..size - 1 {
                // chain vtx0 -> vtx1 -> ... -> vtx999
                graph.new_edge(vtxs[i], props[i], vtxs[i + 1]).unwrap();
            }
            graph
                .edit_vertex(vtxs[0])
                .unwrap()
                .new_attribute(props[0], 0u8)
                .unwrap();
            wr.save_graphs_parallel(vec![graph]).unwrap();
            wr.commit().unwrap();

            let mut query = QueryBuilder::new();
            let mut prev_n = query.match_attr(props[0], 0u8).unwrap();
            for i in 0..size - 1 {
                // chain query to look for n0 <- n1 <- ... <- (n999)
                let n = query.match_outgoing(props[i], prev_n).unwrap();
                prev_n = n;
            }
            query.set_root(prev_n);
            let query = query.compile().unwrap();

            let rd = db.begin_read().unwrap();
            b.iter(|| {
                let res = rd.search(black_box(&query)).unwrap();
                assert_eq!(res, vec![(size - 1) as u64]);
            });
        });
    }
}

criterion_group!(
    benches,
    bench_query_chain,
    bench_supernode,
    bench_union,
    bench_intersection,
);
criterion_main!(benches);
