use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(Protocol, attributes(protocol))]
pub fn protocol_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let has_payload = get_has_payload(&input);

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let name = input.ident;

    quote!(
        impl #impl_generics crate::protocol::Protocol for #name #ty_generics #where_clause {
            const MESSAGE_TYPE: crate::protocol::MessageTypes = crate::protocol::MessageTypes::#name;
            const HAS_PAYLOAD: bool = #has_payload;
        }
    )
    .into()
}

fn get_has_payload(input: &DeriveInput) -> bool {
    match &input.data {
        Data::Struct(data_struct) => !matches!(data_struct.fields, Fields::Unit),
        _ => true,
    }
}
