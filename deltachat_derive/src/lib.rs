#![recursion_limit = "128"]
extern crate proc_macro;

use crate::proc_macro::TokenStream;
use quote::quote;

// For now, assume (not check) that these macroses are applied to enum without
// data.  If this assumption is violated, compiler error will point to
// generated code, which is not very user-friendly.

#[proc_macro_derive(Sqlx)]
pub fn sqlx_derive(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    let name = &ast.ident;

    let gen = quote! {
        impl sqlx::encode::Encode<sqlx::sqlite::Sqlite> for #name {
            fn encode(&self, buf: &mut Vec<sqlx::sqlite::SqliteArgumentValue>) {
                num_traits::ToPrimitive::to_i64(self).expect("invalid type").encode(buf)
            }
        }


        impl<'de> sqlx::decode::Decode<'de, sqlx::sqlite::Sqlite> for #name {
            fn decode(value: sqlx::sqlite::SqliteValue<'de>) -> sqlx::Result<Self> {
                let raw: i64 = sqlx::decode::Decode::decode(value)?;

                Ok(num_traits::FromPrimitive::from_i64(raw).unwrap_or_default())
            }
        }

        impl sqlx::types::Type<sqlx::sqlite::Sqlite> for #name {
            fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
                <i64 as sqlx::types::Type<_>>::type_info()
            }
        }

    };
    gen.into()
}
