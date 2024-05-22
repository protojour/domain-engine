use proc_macro2::TokenStream;
use quote::quote;

use unindent::unindent;

pub fn derive(item: syn::DeriveInput) -> proc_macro2::TokenStream {
    let item_ident = &item.ident;

    match item.data {
        syn::Data::Struct(data) => {
            let arms = data.fields.into_iter().filter_map(|field| {
                let ident_strlit = field.ident?.to_string();
                let comment_lit = extract_doc_comment_arcstr_literal(&field.attrs)?;

                Some(quote! {
                    #ident_strlit => Some(#comment_lit),
                })
            });

            impl_rustdoc(item_ident, quote! { str }, arms)
        }
        syn::Data::Enum(data) => {
            if data
                .variants
                .iter()
                .all(|variant| variant.fields.is_empty())
            {
                let arms = data.variants.into_iter().filter_map(|variant| {
                    let variant_ident = &variant.ident;
                    let comment_lit = extract_doc_comment_arcstr_literal(&variant.attrs)?;

                    Some(quote! {
                        #item_ident::#variant_ident => Some(#comment_lit),
                    })
                });

                impl_rustdoc(item_ident, quote! { Self }, arms)
            } else {
                let arms = data.variants.into_iter().filter_map(|variant| {
                    let ident_strlit = &variant.ident.to_string();
                    let comment_lit = extract_doc_comment_arcstr_literal(&variant.attrs)?;

                    Some(quote! {
                        #ident_strlit => Some(#comment_lit),
                    })
                });

                impl_rustdoc(item_ident, quote! { str }, arms)
            }
        }
        syn::Data::Union(_) => panic!("union not supported"),
    }
}

fn impl_rustdoc(
    item_ident: &syn::Ident,
    key_type: TokenStream,
    arms: impl Iterator<Item = TokenStream>,
) -> TokenStream {
    quote! {
        impl ontol_runtime::rustdoc::RustDoc for #item_ident {
            type Key = #key_type;

            fn get_field_rustdoc(key: &Self::Key) -> Option<arcstr::ArcStr> {
                match key {
                    #(#arms)*
                    _ => None
                }
            }
        }
    }
}

fn extract_doc_comment_arcstr_literal(attrs: &[syn::Attribute]) -> Option<TokenStream> {
    let doc_comments = attrs
        .iter()
        .filter_map(|attr| match attr.meta {
            syn::Meta::NameValue(ref name_value) if name_value.path.is_ident("doc") => {
                Some(&name_value.value)
            }
            _ => None,
        })
        .map(|expr| match expr {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) => s.value(),
            _ => panic!("Doc comment is not a string literal"),
        })
        .collect::<Vec<_>>();

    if doc_comments.is_empty() {
        None
    } else {
        let literal = unindent(doc_comments.join("\n").trim_start());

        Some(quote! { arcstr::literal!(#literal) })
    }
}
