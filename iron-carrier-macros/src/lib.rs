use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Type, parse_macro_input};

#[proc_macro_derive(Protocol, attributes(protocol))]
pub fn protocol_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let has_payload = get_has_payload(&input);
    let response_type = get_response_type(&input);

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let name = input.ident;

    let protocol_impl = quote!(
        impl #impl_generics crate::protocol::Protocol for #name #ty_generics #where_clause {
            const MESSAGE_TYPE: crate::protocol::MessageTypes = crate::protocol::MessageTypes::#name;
            const HAS_PAYLOAD: bool = #has_payload;
        }
    );

    let payload_impl = if has_payload {
        quote!(
            impl #impl_generics crate::protocol::ProtocolPayload for #name #ty_generics #where_clause {}
        )
    } else {
        quote!()
    };

    let query_impl = if let Some(response_type) = response_type {
        quote!(
            impl #impl_generics crate::protocol::ProtocolQuery for #name #ty_generics #where_clause {
                type ResponseType = #response_type;
            }
        )
    } else {
        quote!(
            impl #impl_generics crate::protocol::ProtocolAck for #name #ty_generics #where_clause {
            }
        )
    };

    quote!(
        #protocol_impl
        #payload_impl
        #query_impl
    )
    .into()
}

fn get_has_payload(input: &DeriveInput) -> bool {
    match &input.data {
        Data::Struct(data_struct) => !matches!(data_struct.fields, Fields::Unit),
        _ => true,
    }
}

fn get_response_type(input: &DeriveInput) -> Option<Type> {
    let mut response_type = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("protocol") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("response") {
                let ty: Type = meta.value()?.parse()?;
                response_type = Some(ty);
                Ok(())
            } else {
                Err(meta.error("unsupported protocol attribute, expected `response = Type`"))
            }
        })
        .unwrap_or_else(|err| panic!("failed to parse `#[protocol(..)]` attribute: {err}"));
    }

    response_type
}
