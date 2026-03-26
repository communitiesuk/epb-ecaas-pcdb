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

### Temporary binary for resolving JSON

There is also, while PCDB is represented as a flat file, a binary entry point for this package that can be used to
resolve input JSON.

To convert a JSON document from a local file:

```shell
cargo run -p resolve-products --features=cli -- [PATH_TO_FILE]
```

To convert JSON copied in a Mac OS clipboard:

```shell
pbpaste | cargo run -p resolve-products --features=cli -- -
```

# Contributing

## Using the commit template

If you've done work in a pair or ensemble why not add your co-author(s) to the commit? This way everyone involved is
given credit and people know who they can approach for questions about specific commits. To make this easy there is a
commit template with a list of regular contributors to this code base. You will find it at the root of this
project: `commit_template.txt`. Each row represents a possible co-author, however everyone is commented out by default (
using `#`), and any row that is commented out will not show up in the commit.

### Editing the template

If your name is not in the `commit-template.txt` yet, edit the file and add a new row with your details, following the
format `#Co-Authored-By: Name <email>`, e.g. `#Co-Authored-By: Maja <maja@gmail.com>`. The email must match the email
you use for your GitHub account. To protect your privacy, you can activate and use your noreply GitHub addresses (find
it in GitHub under Settings > Emails > Keep my email addresses private).

### Getting set up

To apply the commit template navigate to the root of the project in a terminal and
use: `git config commit.template commit-template.txt`. This will edit your local git config for the project and apply
the template to every future commit.

### Using the template (committing with co-authors)

When creating a new commit, edit your commit (e.g. using vim, or a code editor) and delete the `#` in front of any
co-author(s) you want to credit. This means that it's probably easier and quicker to use `git commit` (instead
of `git commit -m ""` followed by a `git commit --amend`), as it will show you the commit template content for you to
edit.
