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
    ($p0:expr) => {{
        use sqlx::arguments::Arguments;

        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args
    }};
    ($p0:expr, $p1:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr, $p6:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args.add($p6);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr, $p6:expr, $p7:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args.add($p6);
        args.add($p7);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr, $p6:expr, $p7:expr, $p8:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args.add($p6);
        args.add($p7);
        args.add($p8);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr, $p6:expr, $p7:expr, $p8:expr, $p9:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args.add($p6);
        args.add($p7);
        args.add($p8);
        args.add($p9);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr, $p6:expr, $p7:expr, $p8:expr, $p9:expr, $p10:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args.add($p6);
        args.add($p7);
        args.add($p8);
        args.add($p9);
        args.add($p10);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr, $p6:expr, $p7:expr, $p8:expr, $p9:expr, $p10:expr, $p11:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args.add($p6);
        args.add($p7);
        args.add($p8);
        args.add($p9);
        args.add($p10);
        args.add($p11);
        args
    }};
    ($p0:expr, $p1:expr, $p2:expr, $p3:expr, $p4:expr, $p5:expr, $p6:expr, $p7:expr, $p8:expr, $p9:expr, $p10:expr, $p11:expr, $p12:expr) => {{
        use sqlx::arguments::Arguments;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        args.add($p0);
        args.add($p1);
        args.add($p2);
        args.add($p3);
        args.add($p4);
        args.add($p5);
        args.add($p6);
        args.add($p7);
        args.add($p8);
        args.add($p9);
        args.add($p10);
        args.add($p11);
        args.add($p12);
        args
    }};
}
