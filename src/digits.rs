//! The one way every written form in the crate reads a number back.
//!
//! A number a form writes is a run of ASCII digits and nothing else, so a reader takes only that: a
//! sign is refused rather than quietly taken for what it precedes, since the text a form is read
//! from has to be text that form would have written. `str::parse` alone would take `+7` for `7`,
//! and a reader written that way accepts a file nothing in the crate could have produced.

use core::str::FromStr;

/// Reads `text` as a number written the way the crate writes one: digits, at least one, and nothing
/// else.
///
/// Returns [`None`] for anything else, and for digits the type cannot hold.
pub(crate) fn digits<T: FromStr>(text: &str) -> Option<T> {
    if !is_digits(text) {
        return None;
    }
    text.parse().ok()
}

/// Says whether `text` is written the way the crate writes a number, whether or not any type could
/// hold it — for a reader that reports a number too large apart from one that is not a number.
pub(crate) fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::digits;

    #[test]
    fn only_a_run_of_digits_reads_as_a_number() {
        struct Case {
            name: &'static str,
            text: &'static str,
            want: Option<u8>,
        }
        let cases = [
            Case {
                name: "zero",
                text: "0",
                want: Some(0),
            },
            Case {
                name: "the largest the type holds",
                text: "255",
                want: Some(255),
            },
            Case {
                name: "leading zeros, which every reader has always taken",
                text: "007",
                want: Some(7),
            },
            Case {
                name: "empty",
                text: "",
                want: None,
            },
            Case {
                name: "a plus sign",
                text: "+7",
                want: None,
            },
            Case {
                name: "a minus sign",
                text: "-7",
                want: None,
            },
            Case {
                name: "a space around it",
                text: " 7",
                want: None,
            },
            Case {
                name: "digits from outside ASCII",
                text: "\u{0667}",
                want: None,
            },
            Case {
                name: "more than the type holds",
                text: "256",
                want: None,
            },
        ];
        for case in cases {
            assert_eq!(digits::<u8>(case.text), case.want, "{}", case.name);
        }
    }
}
