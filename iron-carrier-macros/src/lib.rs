use std::hash::Hasher;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, spanned::Spanned};
use syn::{parse_macro_input, DeriveInput};

use siphasher::sip::SipHasher24;

#[proc_macro_derive(HashTypeId, attributes(full_ident))]
pub fn type_id_derive(input: TokenStream) -> TokenStream {
    let content = input.to_string();
    let input = parse_macro_input!(input as DeriveInput);
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let span = input.__span();
    let name = input.ident;

    let type_hash_id = get_type_hash_id(&content, &span);

    quote!(
        impl #impl_generics crate::hash_type_id::HashTypeId for #name #ty_generics #where_clause {
            fn id() -> crate::hash_type_id::TypeId {
                #type_hash_id.into()
            }
        }
    )
    .into()
}
fn get_type_hash_id(content: &str, span: &Span) -> u64 {
    let mut hasher = SipHasher24::new();
    hasher.write(content.as_bytes());

    hasher.write(&span.start().line.to_le_bytes());
    hasher.write(&span.start().column.to_le_bytes());
    hasher.write(&span.end().line.to_le_bytes());
    hasher.write(&span.end().column.to_le_bytes());

    hasher.finish()
}
