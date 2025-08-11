use jsonpath_rust::parser::errors::JsonPathError as OriginalJsonPathError;
use jsonschema::error::ValidationErrorKind;
use jsonschema::ValidationError;
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ResolvePcdbProductsError {
    #[error("Request content could not be parsed as JSON")]
    InvalidJson,
    #[error("Request was considered invalid due to error: {0}")]
    InvalidRequest(#[from] JsonValidationError),
    #[error("Could not extract product references: {0:?}")]
    CouldNotExtractProductReferences(#[from] JsonPathError),
    #[error(
        "There were mismatch errors where provided product references related to incompatible product categories: {0:?}."
    )]
    ProductCategoryMismatches(Vec<String>),
    #[error("Invalid product category reference - expected as string of correct format: {0:?}")]
    InvalidProductCategoryReference(Value),
    #[error("Product reference {0} was not found within the PCDB")]
    UnknownProductReference(String),
}

#[derive(Debug, Error)]
pub struct JsonValidationError {
    value: Value,
    #[allow(dead_code)]
    kind: JsonValidationErrorKind,
    instance_path: String,
    schema_path: String,
}

impl Display for JsonValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The value {:?} at path {} was not valid under its schema at path {}",
            self.value, self.instance_path, self.schema_path
        )
    }
}

impl From<ValidationError<'_>> for JsonValidationError {
    fn from(value: ValidationError) -> Self {
        let ValidationError {
            instance,
            instance_path,
            schema_path,
            kind,
        } = value;
        Self {
            value: instance.into_owned(),
            instance_path: instance_path.to_string(),
            schema_path: schema_path.to_string(),
            kind: kind.into(),
        }
    }
}

/// Simplified mapping from jsonschema::ValidationErrorKind
#[derive(Clone, Debug, PartialEq)]
pub enum JsonValidationErrorKind {
    /// The input array contain more items than expected.
    AdditionalItems { limit: usize },
    /// Unexpected properties.
    AdditionalProperties { unexpected: Vec<String> },
    /// The input value is not valid under any of the schemas listed in the 'anyOf' keyword.
    AnyOf,
    /// Results from a [`fancy_regex::RuntimeError::BacktrackLimitExceeded`] variant when matching
    BacktrackLimitExceeded,
    /// The input value doesn't match expected constant.
    Constant { expected_value: Value },
    /// The input array doesn't contain items conforming to the specified schema.
    Contains,
    /// The input value does not respect the defined contentEncoding
    ContentEncoding { content_encoding: String },
    /// The input value does not respect the defined contentMediaType
    ContentMediaType { content_media_type: String },
    /// Custom error message for user-defined validation.
    Custom { message: String },
    /// The input value doesn't match any of specified options.
    Enum { options: Value },
    /// Value is too large.
    ExclusiveMaximum { limit: Value },
    /// Value is too small.
    ExclusiveMinimum { limit: Value },
    /// Everything is invalid for `false` schema.
    FalseSchema,
    /// When the input doesn't match to the specified format.
    Format { format: String },
    /// May happen in `contentEncoding` validation if `base64` encoded data is invalid.
    FromUtf8 { error: FromUtf8Error },
    /// Too many items in an array.
    MaxItems { limit: u64 },
    /// Value is too large.
    Maximum { limit: Value },
    /// String is too long.
    MaxLength { limit: u64 },
    /// Too many properties in an object.
    MaxProperties { limit: u64 },
    /// Too few items in an array.
    MinItems { limit: u64 },
    /// Value is too small.
    Minimum { limit: Value },
    /// String is too short.
    MinLength { limit: u64 },
    /// Not enough properties in an object.
    MinProperties { limit: u64 },
    /// When some number is not a multiple of another number.
    MultipleOf { multiple_of: f64 },
    /// Negated schema failed validation.
    Not { schema: Value },
    /// The given schema is valid under more than one of the schemas listed in the 'oneOf' keyword.
    OneOfMultipleValid,
    /// The given schema is not valid under any of the schemas listed in the 'oneOf' keyword.
    OneOfNotValid,
    /// When the input doesn't match to a pattern.
    Pattern { pattern: String },
    /// Object property names are invalid.
    PropertyNames { error: String },
    /// When a required property is missing.
    Required { property: Value },
    /// When the input value doesn't match one or multiple required types.
    Type { kind: String },
    /// Unexpected items.
    UnevaluatedItems { unexpected: Vec<String> },
    /// Unexpected properties.
    UnevaluatedProperties { unexpected: Vec<String> },
    /// When the input array has non-unique elements.
    UniqueItems,
    /// Error during schema ref resolution.
    Referencing(String),
}

impl From<ValidationErrorKind> for JsonValidationErrorKind {
    fn from(value: ValidationErrorKind) -> Self {
        match value {
            ValidationErrorKind::AdditionalItems { limit } => Self::AdditionalItems { limit },
            ValidationErrorKind::AdditionalProperties { unexpected } => {
                Self::AdditionalProperties { unexpected }
            }
            ValidationErrorKind::AnyOf { context: _ } => Self::AnyOf,
            ValidationErrorKind::BacktrackLimitExceeded { .. } => Self::BacktrackLimitExceeded,
            ValidationErrorKind::Constant { expected_value } => Self::Constant { expected_value },
            ValidationErrorKind::Contains => Self::Contains,
            ValidationErrorKind::ContentEncoding { content_encoding } => {
                Self::ContentEncoding { content_encoding }
            }
            ValidationErrorKind::ContentMediaType { content_media_type } => {
                Self::ContentMediaType { content_media_type }
            }
            ValidationErrorKind::Custom { message } => Self::Custom { message },
            ValidationErrorKind::Enum { options } => Self::Enum { options },
            ValidationErrorKind::ExclusiveMaximum { limit } => Self::ExclusiveMaximum { limit },
            ValidationErrorKind::ExclusiveMinimum { limit } => Self::ExclusiveMinimum { limit },
            ValidationErrorKind::FalseSchema => Self::FalseSchema,
            ValidationErrorKind::Format { format } => Self::Format { format },
            ValidationErrorKind::FromUtf8 { error } => Self::FromUtf8 { error },
            ValidationErrorKind::MaxItems { limit } => Self::MaxItems { limit },
            ValidationErrorKind::Maximum { limit } => Self::Maximum { limit },
            ValidationErrorKind::MaxLength { limit } => Self::MaxLength { limit },
            ValidationErrorKind::MaxProperties { limit } => Self::MaxProperties { limit },
            ValidationErrorKind::MinItems { limit } => Self::MinItems { limit },
            ValidationErrorKind::Minimum { limit } => Self::Minimum { limit },
            ValidationErrorKind::MinLength { limit } => Self::MinLength { limit },
            ValidationErrorKind::MinProperties { limit } => Self::MinProperties { limit },
            ValidationErrorKind::MultipleOf { multiple_of } => Self::MultipleOf { multiple_of },
            ValidationErrorKind::Not { schema } => Self::Not { schema },
            ValidationErrorKind::OneOfMultipleValid { context: _ } => Self::OneOfMultipleValid,
            ValidationErrorKind::OneOfNotValid { context: _ } => Self::OneOfNotValid,
            ValidationErrorKind::Pattern { pattern } => Self::Pattern { pattern },
            ValidationErrorKind::PropertyNames { error } => Self::PropertyNames {
                error: format!("associated error: {error:?}"),
            },
            ValidationErrorKind::Required { property } => Self::Required { property },
            ValidationErrorKind::Type { kind } => Self::Type {
                kind: format!("kind of type: {kind:?}"),
            },
            ValidationErrorKind::UnevaluatedItems { unexpected } => {
                Self::UnevaluatedItems { unexpected }
            }
            ValidationErrorKind::UnevaluatedProperties { unexpected } => {
                Self::UnevaluatedProperties { unexpected }
            }
            ValidationErrorKind::UniqueItems => Self::UniqueItems,
            ValidationErrorKind::Referencing(referencing) => {
                Self::Referencing(format!("referencing error: {referencing:?}"))
            }
        }
    }
}

/// Simplified mapping for JsonPathError
#[derive(Clone, Debug, Error, PartialEq)]
pub enum JsonPathError {
    #[error("Failed to parse rule from Pest library: {0}")]
    PestError(String),
    #[error("Unexpected rule `{0:?}` when trying to parse `{1}`")]
    UnexpectedRuleLogicError(String, String),
    #[error("Unexpected `none` when trying to parse logic atom: {0} within {1}")]
    UnexpectedNoneLogicError(String, String),
    #[error(
        "Pest returned successful parsing but did not produce any output, that should be unreachable due to .pest definition file: SOI ~ chain ~ EOI"
    )]
    UnexpectedPestOutput,
    #[error("expected a `Rule::path` but found nothing")]
    NoRulePath,
    #[error("expected a `JsonPath::Descent` but found nothing")]
    NoJsonPathDescent,
    #[error("expected a `JsonPath::Field` but found nothing")]
    NoJsonPathField,
    #[error("expected a `f64` or `i64`, but got {0}")]
    InvalidNumber(String),
    #[error("Invalid toplevel rule for JsonPath: {0:?}")]
    InvalidTopLevelRule(String),
    #[error("Failed to get inner pairs for {0}")]
    EmptyInner(String),
    #[error("Invalid json path: {0}")]
    InvalidJsonPath(String),
}

impl From<OriginalJsonPathError> for JsonPathError {
    fn from(value: OriginalJsonPathError) -> Self {
        match value {
            OriginalJsonPathError::PestError(error) => Self::PestError(format!("{error:?}")),
            OriginalJsonPathError::UnexpectedRuleLogicError(rule, parsed) => {
                Self::UnexpectedRuleLogicError(format!("rule: {rule:?}"), parsed)
            }
            OriginalJsonPathError::UnexpectedNoneLogicError(a, b) => {
                Self::UnexpectedNoneLogicError(a, b)
            }
            OriginalJsonPathError::UnexpectedPestOutput => Self::UnexpectedPestOutput,
            OriginalJsonPathError::NoRulePath => Self::NoRulePath,
            OriginalJsonPathError::NoJsonPathDescent => Self::NoJsonPathDescent,
            OriginalJsonPathError::NoJsonPathField => Self::NoJsonPathField,
            OriginalJsonPathError::InvalidNumber(x) => Self::InvalidNumber(x),
            OriginalJsonPathError::InvalidTopLevelRule(rule) => {
                Self::InvalidTopLevelRule(format!("rule: {rule:?}"))
            }
            OriginalJsonPathError::EmptyInner(x) => Self::EmptyInner(x),
            OriginalJsonPathError::InvalidJsonPath(x) => Self::InvalidJsonPath(x),
        }
    }
}
