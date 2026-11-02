/// Keeps key material out of core dumps and away from same-user debuggers.
///
/// A release build aborts on panic and the abort signal dumps core by
/// default, which would write every loaded key and credential to disk. Both
/// calls are best effort: a limit that cannot be set is not worth refusing to
/// run for. The process keeps access to its own `/proc/self`, and children
/// (the keyring command) inherit the zero core limit.
#[cfg(unix)]
#[doc(hidden)]
pub fn harden_process() {
    use rustix::process::{Resource, Rlimit, setrlimit};

    #[cfg(any(target_os = "linux", target_os = "android"))]
    let _ = rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable);
    let _ = setrlimit(
        Resource::Core,
        Rlimit {
            current: Some(0),
            maximum: Some(0),
        },
    );
}
