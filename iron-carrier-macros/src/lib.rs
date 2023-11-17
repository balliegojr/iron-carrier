use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(MessageType)]
pub fn message_type_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let name = input.ident;

    quote!(
        impl #impl_generics crate::message_types::MessageType for #name #ty_generics #where_clause {
            const MESSAGE_TYPE: crate::message_types::MessageTypes = crate::message_types::MessageTypes::#name;
        }
    )
    .into()
}
