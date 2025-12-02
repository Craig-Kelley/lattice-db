#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Handle {
    pub(crate) generation: u32,
    pub(crate) index: usize,
}

#[derive(Debug)]
struct Slot<T> {
    item: Option<T>,
    generation: u32,
}

/// Generational Vector.
/// * Adding items first fills deleted slots, keeping memory small.
/// * Removing items removes all data and frees a slot.
/// * Handles will never point to an item that has replaced a deleted item.
#[derive(Debug)]
pub struct GenVec<T> {
    items: Vec<Slot<T>>,
    freed: Vec<usize>,
}

impl<T> GenVec<T> {
    pub fn new() -> Self {
        Self {
            items: vec![],
            freed: vec![],
        }
    }

    /// Adds an item to the vector.
    pub fn add(&mut self, item: T) -> Handle {
        if let Some(idx) = self.freed.pop() {
            // replacing a freed item
            let slot = &mut self.items[idx];
            slot.item = Some(item);
            Handle {
                generation: slot.generation,
                index: idx,
            }
        } else {
            // no freed items, creates a new slot
            let idx = self.items.len();
            self.items.push(Slot {
                generation: 0,
                item: Some(item),
            });
            Handle {
                generation: 0,
                index: idx,
            }
        }
    }

    /// Removes an item and returns it.
    /// * If item doesn't exist, returns None.
    pub fn remove(&mut self, handle: Handle) -> Option<T> {
        if handle.index >= self.items.len() {
            return None;
        } // item never existed
        let slot = &mut self.items[handle.index];
        if slot.generation != handle.generation {
            return None;
        } // item already removed
        slot.generation += 1; // increment generation for next fill item to use
        self.freed.push(handle.index);
        slot.item.take()
    }

    /// Get a mutable reference to the item.
    /// * If item doesn't exist, returns None.
    pub fn get_mut(&mut self, handle: Handle) -> Option<&mut T> {
        if handle.index >= self.items.len() {
            return None;
        }
        let slot = &mut self.items[handle.index];
        if slot.generation != handle.generation {
            return None;
        }
        slot.item.as_mut()
    }

    /// Get a reference to the item.
    /// * If item doesn't exist, returns None.
    pub fn get(&self, handle: Handle) -> Option<&T> {
        if handle.index >= self.items.len() {
            return None;
        }
        let slot = &self.items[handle.index];
        if slot.generation != handle.generation {
            return None;
        }
        slot.item.as_ref()
    }

    // /// Get the length of the vector.
    // pub fn len(&self) -> usize {
    //     self.items.len() - self.freed.len()
    // }

    /// Iterate through the vector, returning the handle and item.
    pub fn iter(&self) -> impl Iterator<Item = (Handle, &T)> {
        self.items.iter().enumerate().filter_map(|(idx, slot)| {
            slot.item.as_ref().map(|val| {
                (
                    Handle {
                        index: idx,
                        generation: slot.generation,
                    },
                    val,
                )
            })
        })
    }

    // /// Iterate mutably through the vector, returning the handle and item.
    // pub fn iter_mut(&mut self) -> impl Iterator<Item = (Handle, &mut T)> {
    //     self.items.iter_mut().enumerate().filter_map(|(idx, slot)| {
    //         slot.item.as_mut().map(|val| {
    //             (
    //                 Handle {
    //                     index: idx,
    //                     generation: slot.generation,
    //                 },
    //                 val,
    //             )
    //         })
    //     })
    // }

    /// Iterate starting from a specific index.
    pub fn iter_from(&self, start_index: usize) -> impl Iterator<Item = (Handle, &T)> {
        self.items
            .iter()
            .enumerate()
            .skip(start_index)
            .filter_map(|(idx, slot)| {
                slot.item.as_ref().map(|val| {
                    (
                        Handle {
                            index: idx,
                            generation: slot.generation,
                        },
                        val,
                    )
                })
            })
    }

    /// Iterate mutably from a specific index.
    pub fn iter_mut_from(&mut self, start_index: usize) -> impl Iterator<Item = (Handle, &mut T)> {
        self.items
            .iter_mut()
            .enumerate()
            .skip(start_index)
            .filter_map(|(idx, slot)| {
                slot.item.as_mut().map(|val| {
                    (
                        Handle {
                            index: idx,
                            generation: slot.generation,
                        },
                        val,
                    )
                })
            })
    }

    pub(crate) fn get_index(&self, idx: usize) -> Option<&T> {
        if idx >= self.items.len() {
            return None;
        }
        self.items[idx].item.as_ref()
    }

    pub(crate) fn get_mut_index(&mut self, idx: usize) -> Option<&mut T> {
        if idx >= self.items.len() {
            return None;
        }
        self.items[idx].item.as_mut()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_gen_vec_invalid_handle() {
        let mut g = GenVec::new();

        let h1 = g.add(1);
        assert_eq!(h1.index, 0);
        assert_eq!(h1.generation, 0);

        let removed = g.remove(h1);
        assert_eq!(removed, Some(1));

        assert!(g.get_mut(h1).is_none());
        assert!(g.remove(h1).is_none());

        let h2 = g.add(2);
        assert_eq!(h2.index, 0);
        assert_eq!(h2.generation, 1);

        assert!(g.get_mut(h1).is_none());
        assert_eq!(g.get_mut(h2), Some(&mut 2));
    }

    #[test]
    pub fn test_gen_vec_fills() {
        let mut g = GenVec::new();
        let h1 = g.add(1);
        let h2 = g.add(2);

        g.remove(h1);
        g.remove(h2);

        let h3 = g.add(3);
        let h4 = g.add(4);
        let h5 = g.add(5);

        assert_eq!(h3.index, 1);
        assert_eq!(h4.index, 0);
        assert_eq!(h5.index, 2);

        assert_eq!(g.freed.len(), 0);
        assert_eq!(g.items.len(), 3);
    }
}
