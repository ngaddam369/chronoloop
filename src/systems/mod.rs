//! The systems chronoloop runs under simulation.
//!
//! A system is ordinary asynchronous code that takes its time and its randomness from the
//! simulation rather than from the machine, so a run of it is a pure function of its seed.

pub mod pingpong;
