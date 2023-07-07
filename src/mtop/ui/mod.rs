mod compat;
mod core;

pub use crate::ui::core::{initialize_terminal, install_panic_handler, reset_terminal, run, Application};
