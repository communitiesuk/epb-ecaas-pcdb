![test status](https://github.com/communitiesuk/epb-ecaas-pcdb/actions/workflows/test.yml/badge.svg)

# Rust packages for working with the PCDB in the ECaaS project

The [ECaaS project](https://docs.building-energy-calculator.communities.gov.uk) requires integration with a Product
Characteristics Database that is able to resolve references to products that exist in the marketplace of various
categories, and to be able to use the referenced product data to help complete inputs
for [Home Energy Model (HEM)](https://www.gov.uk/government/consultations/home-energy-model-replacement-for-the-standard-assessment-procedure-sap)
calculations.

This repository contains Rust packages to help fulfill these requirements.

## schemagen

The `schemagen` package uses a series of [JSON Patch](https://jsonpatch.com) patches to take a JSON Schema file (such as
those published for the Future Homes Standard, or HEM core) and produce another JSON schema file that defines formats
that will allow relevant parts of the document to include `product_reference` fields that can be used in place of full
product data needed for a HEM-based calculation.

```shell
cargo run -p schemagen [URL of JSON Schema document]
```

## resolve-products

This library package exposes a function `resolve_products` that takes a parameter that implements `std::io::Read` (this
must be a conforming JSON document) and returns another value that implements `std::io::Read` with any
`product_reference` fields resolved into the data necessary for a HEM input.

At the current time the PCDB data backend is a flat file, products.json, that contains three fake air-source heat pump
products. However, it is expected that this package will extended to work with e.g. a database instance.