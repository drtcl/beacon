/// make a PathBuf by joining several parts togther
/// join_path!("/root", "some_dir", "file.txt")
#[macro_export]
macro_rules! join_path {
    ($root:expr, $( $part:expr ),*) => {
        {
            let mut path = PathBuf::from($root);
            $(
                path.push($part);
            )*
            path
        }
    };
}

/// make a PathBuf by joining several parts togther
/// join_path!("/root", "some_dir", "file.txt")
#[macro_export]
macro_rules! join_path_utf8 {
    ($root:expr, $( $part:expr ),*) => {
        {
            let mut path = Utf8PathBuf::from($root);
            $(
                path.push($part);
            )*
            path
        }
    };
}

/// ternary expression
///
/// tern!(condition, true_expr: T, false_expr: T) -> T
#[macro_export]
macro_rules! tern {
    ($cond:expr, $a:expr, $b:expr) => {
        {
            if ($cond) {
                $a
            } else {
                $b
            }
        }
    };
}

/// verbose out, wrapper around println! that only outputs if the bool first arg is true
#[macro_export]
macro_rules! vout {
    ($verbose:expr, $msg:expr) => {
        {
            if $verbose {
                println!($msg);
            }
        }
    };
    ($verbose:expr, $fmt:expr, $($arg:expr),*) => {
        {
            if $verbose {
                println!($fmt, $($arg),*);
            }
        }
    };
}


