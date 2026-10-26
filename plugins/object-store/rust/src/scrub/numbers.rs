//! Card-number checks shared by the card rule and JSON number scrubbing; a
//! port of `isCardNumber` in the reference.

/// Issuer prefixes and lengths: Visa, Mastercard (51-55 and 2221-2720),
/// American Express, Diners Club, JCB, Discover and UnionPay. Lengths are per
/// brand so that, for example, an 18-digit ID starting with 2 is never taken
/// for a Mastercard.
fn known_issuer(d: &str) -> bool {
    let n = d.len();
    if !(13..=19).contains(&n) {
        return false;
    }
    let p = |k: usize| d.get(..k).and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
    let visa = d.starts_with('4') && matches!(n, 13 | 16 | 19);
    let mastercard = ((51..=55).contains(&p(2)) || (2221..=2720).contains(&p(4))) && n == 16;
    let amex = matches!(p(2), 34 | 37) && n == 15;
    let diners =
        ((300..=305).contains(&p(3)) || matches!(p(2), 36 | 38 | 39)) && matches!(n, 14 | 16);
    let jcb = (3528..=3589).contains(&p(4)) && (16..=19).contains(&n);
    let discover_unionpay =
        (p(4) == 6011 || (644..=649).contains(&p(3)) || matches!(p(2), 65 | 62))
            && (16..=19).contains(&n);
    visa || mastercard || amex || diners || jcb || discover_unionpay
}

/// A known issuer prefix and length and a valid Luhn check digit.
fn is_card_digits(digits: &str) -> bool {
    if !known_issuer(digits) {
        return false;
    }
    let sum: u32 = digits
        .bytes()
        .rev()
        .enumerate()
        .map(|(i, b)| {
            let d = u32::from(b - b'0');
            if i % 2 == 0 {
                d
            } else if d * 2 > 9 {
                d * 2 - 9
            } else {
                d * 2
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

/// Whether `text` (digits, optionally grouped by one consistent space or
/// hyphen) is a plausible payment card number.
pub(crate) fn is_card_number(text: &str) -> bool {
    let mut digits = String::with_capacity(text.len());
    let mut sep = None;
    for c in text.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
        } else if sep.is_some_and(|s| s != c) {
            return false;
        } else {
            sep = Some(c);
        }
    }
    is_card_digits(&digits)
}

/// Port of `redactCardsInRun`: redact every card in a run of digit groups
/// separated by single spaces or hyphens. A card starts at a group and spans
/// whole groups joined by one consistent separator; at each group the
/// longest valid card wins. Each start looks at most 19 digits ahead.
fn redact_cards_in_run(run: &str) -> String {
    let groups: Vec<&str> = run.split([' ', '-']).collect();
    let seps: Vec<char> = run.chars().filter(|c| !c.is_ascii_digit()).collect();
    let mut out = String::with_capacity(run.len());
    let mut i = 0;
    while i < groups.len() {
        let end = card_end(&groups, &seps, i);
        out.push_str(end.map_or(groups[i], |_| super::rules::REDACTED));
        let next = end.map_or(i + 1, |e| e + 1);
        if next < groups.len() {
            out.push(seps[next - 1]);
        }
        i = next;
    }
    out
}

/// The last group of the longest valid card starting at group `i`.
fn card_end(groups: &[&str], seps: &[char], i: usize) -> Option<usize> {
    // Every issuer prefix starts with 2-6.
    if !groups[i].starts_with(['2', '3', '4', '5', '6']) {
        return None;
    }
    let mut digits = String::with_capacity(19);
    let mut end = None;
    for j in i..groups.len() {
        if j > i + 1 && seps[j - 1] != seps[i] {
            break;
        }
        digits.push_str(groups[j]);
        if digits.len() > 19 {
            break;
        }
        if digits.len() >= 13 && is_card_digits(&digits) {
            end = Some(j);
        }
    }
    end
}

/// Gate for the card rule: some run of digit groups (single spaces or hyphens
/// between digits) holds at least 13 digits, the shortest card.
pub(crate) fn has_card_length_run(text: &str) -> bool {
    let b = text.as_bytes();
    let mut run = 0;
    for (i, &c) in b.iter().enumerate() {
        if c.is_ascii_digit() {
            run += 1;
            if run >= 13 {
                return true;
            }
        } else {
            let joins = (c == b' ' || c == b'-')
                && i > 0
                && b[i - 1].is_ascii_digit()
                && b.get(i + 1).is_some_and(u8::is_ascii_digit);
            if !joins {
                run = 0;
            }
        }
    }
    false
}

/// Gate for the E.164 rule: a `+`, a digit 1-9, and at least 8 digits in all
/// before anything other than a digit, space, `.`, `-`, `(` or `)`.
pub(crate) fn has_e164_shape(text: &str) -> bool {
    let b = text.as_bytes();
    b.iter().enumerate().any(|(i, &c)| {
        c == b'+'
            && b.get(i + 1).is_some_and(|d| (b'1'..=b'9').contains(d))
            && b[i + 1..]
                .iter()
                .take_while(|c| c.is_ascii_digit() || b" .-()".contains(c))
                .filter(|c| c.is_ascii_digit())
                .nth(7)
                .is_some()
    })
}

/// Gate for the North American rule: `ddd[.-]ddd[.-]dddd` or `(ddd)` followed
/// (after an optional space) by `ddd[.-]dddd`.
pub(crate) fn has_nanp_shape(text: &str) -> bool {
    let b = text.as_bytes();
    let digits = |at: usize, n: usize| {
        b.get(at..at + n)
            .is_some_and(|s| s.iter().all(u8::is_ascii_digit))
    };
    let sep = |at: usize| b.get(at).is_some_and(|c| *c == b'.' || *c == b'-');
    let tail = |at: usize| digits(at, 3) && sep(at + 3) && digits(at + 4, 4);
    (0..b.len()).any(|i| {
        (digits(i, 3) && sep(i + 3) && tail(i + 4))
            || (b[i] == b'('
                && digits(i + 1, 3)
                && b.get(i + 4) == Some(&b')')
                && (tail(i + 5) || (b.get(i + 5) == Some(&b' ') && tail(i + 6))))
    })
}

/// The card rule's replacer: the match is an optional lead character (never a
/// digit) followed by a run of digit groups.
pub(crate) fn redact_cards(m: &str) -> String {
    let split = m
        .chars()
        .next()
        .filter(|c| !c.is_ascii_digit())
        .map_or(0, char::len_utf8);
    let (lead, run) = m.split_at(split);
    format!("{lead}{}", redact_cards_in_run(run))
}

#[cfg(test)]
mod tests {
    use super::{is_card_number as card, redact_cards};

    #[test]
    fn cards_by_issuer_length_and_luhn() {
        for ok in [
            "4111111111111111",
            "4111 1111 1111 1111",
            "4111-1111-1111-1111",
            "5555555555554444",
            "2223003122003222",
            "378282246310005",
            "3782 822463 10005",
            "6011111111111117",
            "3530111333300000",
            "30569309025904",
        ] {
            assert!(card(ok), "{ok}");
        }
        for no in [
            "4111111111111112",
            "9111111111111111",
            "411111111111111",
            "4111 1111-1111 1111",
            "240329225065000960",
            "1729900000000",
        ] {
            assert!(!card(no), "{no}");
        }
    }

    #[test]
    fn every_card_in_a_run_is_redacted() {
        for (m, want) in [
            ("4111 1111 1111 1111 12", "[REDACTED] 12"),
            (" 4111111111111111 123", " [REDACTED] 123"),
            ("4111111111111111 5555555555554444", "[REDACTED] [REDACTED]"),
            ("415 555 0123 4111111111111111", "415 555 0123 [REDACTED]"),
            ("41111111111111110", "41111111111111110"),
        ] {
            assert_eq!(redact_cards(m), want, "{m}");
        }
    }
}
