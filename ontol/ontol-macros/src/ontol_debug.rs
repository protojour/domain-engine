use quote::quote;

pub fn derive(item: syn::DeriveInput) -> proc_macro2::TokenStream {
    let (ident, generics, body) = match item.data {
        syn::Data::Struct(data) => {
            let body = fields_debug_body(&item.ident, &data.fields);
            let match_arm = fields_match_arm(&quote!(), &item.ident, &data.fields, body);
            (
                &item.ident,
                &item.generics,
                quote! {
                    match self { #match_arm }
                },
            )
        }
        syn::Data::Enum(data) => {
            let arm_prefix = quote! { Self:: };

            let arms = data.variants.iter().map(|variant| {
                let body = fields_debug_body(&variant.ident, &variant.fields);
                fields_match_arm(&arm_prefix, &variant.ident, &variant.fields, body)
            });

            (
                &item.ident,
                &item.generics,
                quote! {
                    match self { #(#arms)* }
                },
            )
        }
        _ => panic!("not supported"),
    };

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ontol_runtime::debug::OntolDebug for #ident #ty_generics #where_clause {
            fn fmt(&self, __ofmt: &dyn ontol_runtime::debug::OntolFormatter, __fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
                #body
            }
        }
    }
}

fn fields_match_arm(
    prefix: &proc_macro2::TokenStream,
    ident: &syn::Ident,
    fields: &syn::Fields,
    body: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    match fields {
        syn::Fields::Named(named) => {
            let bindings = named.named.iter().map(|field| &field.ident);
            quote! {
                #prefix #ident { #(#bindings),* } => { #body }
            }
        }
        syn::Fields::Unnamed(unnamed) => {
            let bindings = unnamed
                .unnamed
                .iter()
                .enumerate()
                .map(|(index, _)| quote::format_ident!("f{index}"));
            quote! {
                #prefix #ident(#(#bindings),*) => { #body }
            }
        }
        syn::Fields::Unit => {
            quote! {
                #prefix #ident => { #body }
            }
        }
    }
}

fn fields_debug_body(ident: &syn::Ident, fields: &syn::Fields) -> proc_macro2::TokenStream {
    let lit = ident_lit(ident);
    match fields {
        syn::Fields::Named(named) => {
            let fields = named.named.iter().map(|named| {
                let ident = named.ident.as_ref().unwrap();
                let lit = ident_lit(ident);
                quote! {
                    .field(#lit, &ontol_runtime::debug::Fmt(__ofmt, #ident))
                }
            });

            quote! {
                __fmt.debug_struct(#lit) #(#fields)* .finish()
            }
        }
        syn::Fields::Unnamed(unnamed) => {
            let fields = unnamed.unnamed.iter().enumerate().map(|(index, _)| {
                let ident = quote::format_ident!("f{index}");
                quote! {
                    .field(&ontol_runtime::debug::Fmt(__ofmt, #ident))
                }
            });

            quote! {
                __fmt.debug_tuple(#lit) #(#fields)* .finish()
            }
        }
        syn::Fields::Unit => {
            quote! {
                __fmt.write_str(#lit)
            }
        }
    }
}

fn ident_lit(ident: &syn::Ident) -> syn::LitStr {
    syn::LitStr::new(&format!("{}", ident), ident.span())
}
