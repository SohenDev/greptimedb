use std::panic;

use backtrace::Backtrace;
use metrics::increment_counter;
use tracing::error;

pub fn set_panic_hook() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic| {
        let backtrace = Backtrace::new();
        let backtrace = format!("{backtrace:?}");
        if let Some(location) = panic.location() {
            error!(
                message = %panic,
                backtrace = %backtrace,
                panic.file = location.file(),
                panic.line = location.line(),
                panic.column = location.column(),
            );
        } else {
            error!(message = %panic, backtrace = %backtrace);
        }
        increment_counter!("panic_counter");
        default_hook(panic);
    }));
}
