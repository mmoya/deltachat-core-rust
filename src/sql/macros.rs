#[macro_export]
macro_rules! paramsv {
    () => {
        Vec::new()
    };
    ($($param:expr),+ $(,)?) => {
        vec![$(&$param as &dyn $crate::ToSql),+]
    };
}

#[macro_export]
macro_rules! paramsx {
    () => {
        sqlx::sqlite::SqliteArguments::default()
    };
    ($($param:expr),+ $(,)?) => {{
        use sqlx::arguments::Arguments;

        let mut args = sqlx::sqlite::SqliteArguments::default();
        $(args.add($param);)+
        args
    }};
}
