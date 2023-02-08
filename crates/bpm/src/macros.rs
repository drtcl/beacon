/// make a PathBuf by joining several parts togther
/// join_path!("/root", "some_dir", "file.txt")
#[macro_export]
macro_rules! join_path {
    ( $root:expr, $( $part:expr ),* ) => {
        {
            let mut path = PathBuf::from($root);
            $(
                path.push($part);
            )*
            path
        }
    }
}

#[macro_export]
macro_rules! tern{
    ( $cond:expr, $a:expr, $b:expr ) => {
        {
            if ($cond) {
                $a
            } else {
                $b
            }
        }
    }
}
