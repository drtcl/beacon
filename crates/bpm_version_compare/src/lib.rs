// bpm version compare
//
// --- General rules ---
//
// 1. Less parts is LESS than more parts (Less parts is Lesser)/(greater parts is greater)
// 2.    numeric is LESS than Non-numeric
// 3.     prerel is LESS than non-prerel
// 4.  non-build is LESS than build
// 5.     a mess is LESS than a non-mess
//
// --- General Form ---
//
// numbers                              example: 1.2.3
// numbers "-" prerelease               example: 1.2.3-rc1
// numbers "+" build                    example: 1.2.3+20250102
// numbers "-" prerelease "+" build     example: 1.2.3-rc1+20250102
// numbers mess                         example: 1.2.3-rc1::b
// mess                                 exmaple: 2a.1.3::b
//
// One or more dot separated numbers
// - followed by optional prerelease string (indicated by "-" separator)
// - followed by optional build string (indicated by "+" separator)
//
// --- Differences from Semver ---
//
// 1. Allow any number of leading numbers, where semver requires exactly 3.
//    example: 1, 1.2, 1.2.3, 1.2.3.4
//
// 2. Semver treats '-' as an alpha character, and compares it asciibetically.
//    example: 1.2.3-1-1 < 1.2.3-1-10 < 1.2.3-1-2
//             "1-1" < "1-10" < "1-2"
//    Instead, we split on '-' and continue numeric compares
//    example: 1.2.3-1-1 < 1.2.3-1-2 < 1.2.3-1-10
//             (1,1) < (1,2) < (1,10)
//
// 3. We allow leading zeros in numbers. This IS nasty and should avoided, but we do our best to
//    compare without error. The zeros are ignored until they are need to break ties. Zeros are
//    then considered less than non-leading zeros.
//
//    example: 1.1 < 1.02 < 1.2 < 1.003 < 1.03 < 1.3
//
// 4. A leading 'v' or 'V' will be ignored
//
// 5. We also try to compare and order any mess without throwing an error
//    example:
//         6.1.2a-2      ((6, 1), ("2a", 2))
//       < 6.1.2a-10     ((6, 1), ("2a", 10))
//       < 6.1:4a        ((6, 1), ("4a"))
//       < 6.1++0:=:rc1  ((6, 2), (0, "rc1"))
//
// --- Details ---
//
// 1. What defines a mess?
//     In the leading numbers part (dot separated nums),
//        if a number is non-numeric, that starts the mess (to the end of the string)
//        for total order comparison, the previous separator is also included.
//
//        example: 1.2.3a-rc1
//                 becomes (1,2) + mess of ".3a-rc1"
//
//     otherwise, 2 consecutive non-alphanumeric characters will cause a mess.
//     The prerelease and build part will not be present if there is a mess.
//
//        example: 1.2.3-rc1+2025::1
//                 becomes (1,2,3) + mess of "-rc1+2025::1"
//            and: 1.2.3=rc2-3
//                 becomes (1,2,3) + mess of "=rc2-3"
//
// 2. How are messes compared?
//
//     example: 1.2.3-rc1+2025::1   ((1,2,3), ("rc1", 2025, 1))
//         and  1.2.3=rc2-3         ((1,2,3), ("rc2", 3))
//
//
//
// --- Grammar --- (Mess not included)
//
// <version> = <semver_version>
//           | <relaxed_version>
//
// <rerelaxed_version> = <relaxed_numeric_identifiers>
//                     | <relaxed_numeric_identifiers> "-" <pre_release>
//                     | <relaxed_numeric_identifiers> "+" <build>
//                     | <relaxed_numeric_identifiers> "-" <pre_release> "+" <build>
//
// <relaxed_numeric_identifiers> = <relaxed_numeric_identifier>
//                               | <relaxed_numeric_identifier> <relaxed_numeric_identifiers>
//
// <relaxed_numeric_identifier> = <digits>
//
// --- below is entirely semver ---
//
// <semver_version> = <version_core>
//                  | <version_core> "-" <pre_release>
//                  | <version_core> "+" <build>
//                  | <version_core> "-" <pre_release> "+" <build>
//
// <version_core> = <major> "." <minor> "." <patch>
//
// <major> = <numeric_identifier>
//
// <minor> = <numeric_identifier>
//
// <patch> = <numeric_identifier>
//
// <pre_release> = <dot_separated_pre_release_identifiers>
//
// <dot_separated_pre_release_identifiers> = <pre_release_identifier>
//                                         | <pre_release_identifier> "." <dot_separated_pre_release_identifiers>
//
// <build> = <dot_separated_build_identifiers>
//
// <dot_separated_build_identifiers> = <build_identifier>
//                                   | <build_identifier> "." <dot_separated_build_identifiers>
//
// <pre_release_identifier> = <alphanumeric_identifier>
//                          | <numeric_identifier>
//
// <build_identifier> = <alphanumeric_identifier>
//                    | <digits>
//
// <alphanumeric_identifier> = <non_digit>
//                           | <non_digit> <identifier_characters>
//                           | <identifier_characters> <non_digit>
//                           | <identifier_characters> <non_digit> <identifier_characters>
//
// <numeric_identifier> = "0"
//                      | <positive_digit>
//                      | <positive_digit> <digits>
//
// <identifier_characters> = <identifier_character>
//                         | <identifier_character> <identifier_characters>
//
// <identifier_character> = <digit> | <non_digit>
//
// <non_digit> = <letter> | "-"
//
// <digits> = <digit>
//          | <digit> <digits>
//
// <digit> = "0"
//         | <positive_digit>
//
// <positive_digit> = "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
//
// <letter> = "A" | "B" | "C" | "D" | "E" | "F" | "G" | "H" | "I" | "J" | "K" | "L" | "M"
//          | "N" | "O" | "P" | "Q" | "R" | "S" | "T" | "U" | "V" | "W" | "X" | "Y" | "Z"
//          | "a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i" | "j" | "k" | "l" | "m"
//          | "n" | "o" | "p" | "q" | "r" | "s" | "t" | "u" | "v" | "w" | "x" | "y" | "z"



#[cfg(not(any(feature="eager-mess", feature="lazy-mess")))]
compile_error!("feature eager-mess or lazy-mess required");
#[cfg(all(feature="eager-mess", feature="lazy-mess"))]
compile_error!("Use only one of eager-mess or lazy-mess required");

const NORMAL_SEPS: &[char] = &['.', '-', '+'];

fn is_normal_sep(c: char) -> bool {
    NORMAL_SEPS.contains(&c)
}

fn is_normal_char(c: char) -> bool {
    c.is_alphanumeric() || is_normal_sep(c)
}

fn is_mess_sep(c: char) -> bool {
    !is_normal_char(c)
}

fn is_sep(c: char) -> bool {
    is_normal_sep(c) || is_mess_sep(c)
}

fn is_mess(s: &str) -> bool {

    // must
    // 1. start with alphanumeric
    // 2. end with alphanumeric
    // 3. not have 2 consecutive non-alphanumeric chars
    // 4. be entirely alphanumeric or - + .

    // 1
    let mut chars = s.chars();
    if let Some(first) = chars.next() && !first.is_alphanumeric() {
        return true;
    }
    // 2
    if let Some(last) = chars.last() && !last.is_alphanumeric() {
        return true;
    }

    // 3
    let chars = s.chars();
    let mut last_is_sep = false;
    for c in chars {

        // 4
        if !is_normal_char(c) {
            return true;
        }

        if is_sep(c) {
            if last_is_sep {
                return true;
            }
            last_is_sep = true;
        } else {
            last_is_sep = false;
        }
    }

    false
}

fn eq_to_none(v: std::cmp::Ordering) -> Option<std::cmp::Ordering> {
    if matches!(v, std::cmp::Ordering::Equal) {
        return None;
    }
    Some(v)
}

#[cfg(not(feature="dict"))]
fn str_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.cmp(b)
}

#[derive(Clone)]
pub struct DictIter<'a> {
    s: &'a str,
}
pub fn dict_iter<'a>(s: &'a str) -> DictIter<'a> {
    DictIter { s }
}
impl<'a> Iterator for DictIter<'a> {
    type Item = (&'a str, bool);
    fn next(&mut self) -> Option<Self::Item> {

        // if the first char is numeric, chop off all leading consecutive numerics

        if !self.s.is_empty() {
            let mut first = true;
            let mut part_is_numeric = false;
            for (idx, c) in self.s.char_indices() {
                let numeric = c.is_ascii_digit();
                if first {
                    part_is_numeric = numeric;
                    first = false;
                } else if part_is_numeric == numeric {
                    // good, include it
                } else {
                    // mismatch, return what we had, update internal string ref
                    let part = &self.s[0..idx];
                    self.s = &self.s[idx..];
                    return Some((part, part_is_numeric));
                }
            }
            // if here, then we've scanned the entire string and
            // it is entirely numeric or non-numeric
            let part = self.s;
            self.s = &self.s[0..0];
            return Some((part, part_is_numeric));
        }
        None
    }
}

#[cfg(feature="dict")]
fn str_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    str_dict_cmp(a, b)
}

#[cfg(feature="dict")]
fn str_dict_cmp(a: &str, b: &str) -> std::cmp::Ordering {

    //eprintln!("dict compare\n  {a}\n  {b}");

    let mut a_parts = dict_iter(a).map(|(s, numeric)| (s, numeric.then(|| s.parse::<u64>().ok()).flatten()));
    let mut b_parts = dict_iter(b).map(|(s, numeric)| (s, numeric.then(|| s.parse::<u64>().ok()).flatten()));

    loop {
        match (a_parts.next(), b_parts.next()) {
            (None, None) => {
                return std::cmp::Ordering::Equal;
            }
            (None, Some(_)) => {
                return std::cmp::Ordering::Less;
            }
            (Some(_), None) => {
                return std::cmp::Ordering::Greater;
            }
            (Some((sa, na)), Some((sb, nb))) => {
                match (na, nb) {
                    (None, Some(_)) |
                    (Some(_), None) |
                    (None, None) => {
                        if let Some(ret) = eq_to_none(sa.cmp(sb)) {
                            return ret;
                        }
                    }
                    (Some(left), Some(right)) => {
                        if let Some(ret) = eq_to_none(left.cmp(&right)) {
                            return ret;
                        }
                    }
                }
            }
        }
    }
}

pub fn scan_cmp(v1: &str, v2: &str) -> std::cmp::Ordering {

    //eprintln!("scan_cmp {v1}\n         {v2}");

    let mut tie_breaker = None;

    let mut parts1 = scan_it(v1);
    let mut parts2 = scan_it(v2);

    loop {

        let part1 = parts1.next();
        let part2 = parts2.next();

        match (part1, part2) {

            // compare nums
            (Some(ScanToken::Num{n: n1, raw: raw1}), Some(ScanToken::Num{n: n2, raw: raw2})) => {
                if n1 != n2 {
                    return n1.cmp(&n2);
                }
                tie_breaker = tie_breaker.or_else(|| eq_to_none(raw1.cmp(raw2)));
            }

            // unmatched count of numbers
            (_, Some(ScanToken::Num{..}))  => {
                // left side ran out of numbers, less
                return std::cmp::Ordering::Less;
            }
            (Some(ScanToken::Num{..}), _)  => {
                // right side ran out of numbers, greater
                return std::cmp::Ordering::Greater;
            }

            // compare prerel to prerel
            (Some(ScanToken::Prerel(pre1)), Some(ScanToken::Prerel(pre2))) => {
                if let Some(ret) = eq_to_none(part_cmp(pre1, pre2)) {
                    return ret;
                }
            }

            // compare build to build
            (Some(ScanToken::Build(bld1)), Some(ScanToken::Build(bld2))) => {
                if let Some(ret) = eq_to_none(part_cmp(bld1, bld2)) {
                    return ret;
                }
            }

            // compare mess to mess
            (Some(ScanToken::Mess(m1)), Some(ScanToken::Mess(m2))) => {
                if let Some(ret) = eq_to_none(mess_cmp(m1, m2)) {
                    return ret;
                }
            }

            // a mess is always less than a non-mess
            (Some(ScanToken::Mess(_)), _) => {
                return std::cmp::Ordering::Less;
            }
            (_, Some(ScanToken::Mess(_))) => {
                return std::cmp::Ordering::Greater;
            }

            (Some(ScanToken::Prerel(_)), _) => {
                // left side is prerel, right is not, less
                return std::cmp::Ordering::Less;
            }
            (_, Some(ScanToken::Prerel(_))) => {
                // right side is prerel, left is not, greater
                return std::cmp::Ordering::Greater;
            }

            (_, Some(ScanToken::Build(_))) => {
                // right side has build, left does not, less
                return std::cmp::Ordering::Less;
            }
            (Some(ScanToken::Build(_)), _) => {
                // left side has build, right does not, greater
                return std::cmp::Ordering::Greater;
            }

            (None, None) => {
                break;
            }
        }
    }

    if let Some(tie_breaker) = tie_breaker {
        return tie_breaker;
    }

    //v1.cmp(v2)
    std::cmp::Ordering::Equal
}

enum NumericNonNumeric<'a> {
    Numeric {
        n: u64,
        raw: &'a str
    },
    NonNumeric(&'a str),
}

fn map_to_numeric<'a>(s: Option<&'a str>) -> Option<NumericNonNumeric<'a>> {
    s.map(|s| {
        if let Ok(n) = s.parse::<u64>() {
            NumericNonNumeric::Numeric { n, raw: s }
        } else {
            NumericNonNumeric::NonNumeric(s)
        }
    })
}

// "a.b-c.d" -> "(("a", "b"), ("c", "d"))"
// "a-b.c-d" -> "(("a"), ("b", "c"), ("d"))"
pub fn explain_parts(s: &str, mess: bool) -> String {

    let mut exp = String::new();
    exp.push('(');
    s.split(|c| (mess && is_sep(c)) || c == '-').for_each(|o_part| {
        exp.push('(');
        o_part.split('.').for_each(|i_part| {
            exp.push('"');
            exp.push_str(i_part);
            exp.push('"');
            exp.push(',');
            exp.push(' ');
        });
        exp.pop();
        exp.pop();
        exp.push(')');
        exp.push(',');
        exp.push(' ');
    });
    exp.pop();
    exp.pop();
    exp.push(')');
    exp
}

// This is used for prerelease and build strings
//
// first order: comparing on parts separated by '-'
// second order: comparing on parts separated by '.'
//
// - parts consisting of only digits are compare numerically
// - parts containing letters are compared in ASCII order
// - numeric parts are lesser than non-numeric parts
// - a part with a lower field count is lesser than a part with a greater field count
//      1.0.0-alpha < 1.0.0-alpha.1
fn part_cmp(v1: &str, v2: &str) -> std::cmp::Ordering {

    if v1 == v2 {
        return std::cmp::Ordering::Equal;
    }

    let dash_parts1 = v1.split('-').map(Some);
    let dash_parts2 = v2.split('-').map(Some);

    let count1 = dash_parts1.clone().count();
    let count2 = dash_parts2.clone().count();
    let max_count = std::cmp::max(count1, count2);

    let dash_parts1 = dash_parts1.chain(std::iter::repeat(None)).take(max_count);
    let dash_parts2 = dash_parts2.chain(std::iter::repeat(None)).take(max_count);

    let mut tie_breaker = None;

    for (dash_part1, dash_part2) in std::iter::zip(dash_parts1, dash_parts2) {
        //println!("first order: {dash_part1:?} and {dash_part2:?}");

        if dash_part1.is_none() {
            return std::cmp::Ordering::Less;
        }
        if dash_part2.is_none() {
            return std::cmp::Ordering::Greater;
        }

        let dash_part1 = dash_part1.unwrap();
        let dash_part2 = dash_part2.unwrap();

        let dot_parts1 = dash_part1.split('.').map(Some);
        let dot_parts2 = dash_part2.split('.').map(Some);

        let count1 = dot_parts1.clone().count();
        let count2 = dot_parts2.clone().count();
        let max_count = std::cmp::max(count1, count2);

        let dot_parts1 = dot_parts1.chain(std::iter::repeat(None)).take(max_count);
        let dot_parts2 = dot_parts2.chain(std::iter::repeat(None)).take(max_count);

        let dot_parts1 = dot_parts1.map(map_to_numeric);
        let dot_parts2 = dot_parts2.map(map_to_numeric);

        for (left, right) in std::iter::zip(dot_parts1, dot_parts2) {
            //println!("second order: {left:?} and {right:?}");

            match (left, right) {
                (None, _) => {
                    return std::cmp::Ordering::Less;
                }
                (_, None) => {
                    return std::cmp::Ordering::Greater;
                }
                (Some(NumericNonNumeric::Numeric{n: n1, raw: raw1}), Some(NumericNonNumeric::Numeric{n: n2, raw: raw2})) => {
                    // both numeric
                    if n1 != n2 {
                        return n1.cmp(&n2);
                    }
                    // tie breaker for leading zeros (007 < 7)
                    tie_breaker = tie_breaker.or_else(|| eq_to_none(raw1.cmp(raw2)));
                }
                (Some(NumericNonNumeric::NonNumeric(_)), Some(NumericNonNumeric::Numeric{..})) => {
                    // left is non-numeric, right is numeric
                    // left > right
                    return std::cmp::Ordering::Greater;
                }
                (Some(NumericNonNumeric::Numeric{..}), Some(NumericNonNumeric::NonNumeric(_))) => {
                    // left is numeric, right is non-numeric
                    // left < right
                    return std::cmp::Ordering::Less;
                }
                (Some(NumericNonNumeric::NonNumeric(left)), Some(NumericNonNumeric::NonNumeric(right))) => {
                    // both non-numeric
                    if let Some(ret) = eq_to_none(str_cmp(left, right)) {
                        return ret;
                    }
                }
            }
        }
    }

    if let Some(tie_breaker) = tie_breaker {
        return tie_breaker;
    }

    std::cmp::Ordering::Equal
}

fn mess_cmp(v1: &str, v2: &str) -> std::cmp::Ordering {

    //eprintln!("mess_cmp: {v1} and {v2}");

    if v1 == v2 {
        return std::cmp::Ordering::Equal;
    }

    let parts1 = v1.split(is_sep).filter(|s| !s.is_empty()).map(Some);
    let parts2 = v2.split(is_sep).filter(|s| !s.is_empty()).map(Some);

    let count1 = parts1.clone().count();
    let count2 = parts2.clone().count();
    let max_count = std::cmp::max(count1, count2);

    let parts1 = parts1.chain(std::iter::repeat(None)).take(max_count);
    let parts2 = parts2.chain(std::iter::repeat(None)).take(max_count);

    let mut tie_breaker = None;

    for (part1, part2) in std::iter::zip(parts1, parts2) {

        match (part1, part2) {
            (Some(part1), Some(part2)) => {

                let num1 = part1.parse::<u64>();
                let num2 = part2.parse::<u64>();

                match (num1, num2) {
                    (Ok(num1), Ok(num2)) => {
                        // both numeric
                        if num1 != num2 {
                            return num1.cmp(&num2);
                        }
                        tie_breaker = tie_breaker.or_else(|| eq_to_none(part1.cmp(part2)));
                    }
                    (Err(_), Ok(_)) => {
                        // left is non-numeric, right is numeric
                        // left > right
                        return std::cmp::Ordering::Greater;
                    }
                    (Ok(_), Err(_)) => {
                        // left is numeric, right is non-numeric
                        // left < right
                        return std::cmp::Ordering::Less;
                    }
                    (Err(_), Err(_)) => {
                        // both non-numeric
                        if let Some(ret) = eq_to_none(str_cmp(part1, part2)) {
                            return ret;
                        }
                    }
                }
            }
            (None, Some(_)) => {
                return std::cmp::Ordering::Less;
            }
            (Some(_), None) => {
                return std::cmp::Ordering::Greater;
            }
            (None, None) => {
                // unreachable
            }
        }
    }

    if let Some(tie_breaker) = tie_breaker {
        return tie_breaker;
    }

    str_cmp(v1, v2)
}

// --- ScanIterator --------------------------------------------------

#[derive(Debug)]
pub enum ScanToken<'a> {
    Num {
        n: u64,
        raw: &'a str,
    },
    Prerel(&'a str),
    Build(&'a str),
    Mess(&'a str)
}

#[derive(PartialEq, Debug)]
#[repr(u32)]
enum ScanStage {
    Nums = 0,
    Prerel = 1,
    Build = 2,
    Mess = 3,
    Done = 4,
}

pub struct ScanIterator<'a> {
    v: &'a str,
    idx: usize,
    stage: ScanStage,
    mess_start: Option<usize>,
    consumed: usize,
    char_count: usize,
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = ScanToken<'a>;
    fn next(&mut self) -> Option<Self::Item> {

        if self.stage == ScanStage::Done {
            return None;
        }

        let remaining = &self.v[self.idx..];

        //eprintln!("V:         <{}>", self.v);
        //eprintln!("REMAINING: {}<{}>", " ".repeat(self.v.len() - remaining.len()), remaining);
        //eprintln!("stage      {:?}", self.stage);

        if self.stage == ScanStage::Nums {

            let mut prev_sep;
            let mut cur_sep = true;

            let mut part_start_idx = 0;
            let mut in_part = false;

            for (nth, (idx, c)) in self.v.char_indices().enumerate().skip(self.consumed) {

                self.idx = idx;
                self.consumed = nth + 1;

                prev_sep = cur_sep;
                cur_sep = is_sep(c);
                let cur_is_number = c.is_ascii_digit();

                let is_last_char = (1+nth) == self.char_count;

                // 2 adjacent seps, that's a mess
                if prev_sep && cur_sep {
                    let mess = &self.v[idx..];
                    self.stage = ScanStage::Done;
                    return Some(ScanToken::Mess(mess));
                }

                let mut full_part = None;

                // starting a part, idx point to first digit
                if prev_sep && !cur_sep {
                    part_start_idx = idx;
                    in_part = true;
                }

                // ending a part, idx points to sep char after last digit
                if in_part && cur_sep {
                    in_part = false;
                    full_part = Some(&self.v[part_start_idx..idx]);
                    self.mess_start = Some(idx);

                    // what to do based on the separator char
                    self.stage = match c {
                        '.' => ScanStage::Nums,   // continue parsing nums
                        '-' => ScanStage::Prerel, // start parsing prerelease
                        '+' => ScanStage::Build,  // start parsing build
                        _   => ScanStage::Mess,   // give up, mess
                    };

                } else if in_part && is_last_char {
                    // -- end of a part, and end of full string
                    self.stage = ScanStage::Done;
                    full_part = Some(&self.v[part_start_idx..=idx]);
                }


                // example: full string = "3.14.0a"
                //             mess starts at ".0a"
                #[cfg(feature="eager-mess")]
                if in_part && !cur_is_number {
                    // hit a non-numeric character, that's a mess
                    self.stage = ScanStage::Done;
                    let mess_start = self.mess_start.unwrap_or(part_start_idx);
                    let mess = &self.v[mess_start..];
                    return Some(ScanToken::Mess(mess));
                }

                // example: full string = "3.14.0a"
                //             mess starts at   "a"
                #[cfg(feature="lazy-mess")]
                if in_part && !cur_is_number {
                    // hit a non-numeric character, that's a mess
                    self.stage = ScanStage::Mess;
                    full_part = Some(&self.v[part_start_idx..idx]);
                    self.mess_start = Some(idx);
                }

                if let Some(this_part) = full_part {
                    if let Ok(n) = this_part.parse::<u64>() {
                        self.idx += c.len_utf8();
                        return Some(ScanToken::Num{n, raw: this_part});
                    } else {
                        self.stage = ScanStage::Done;
                        let mess = &self.v[part_start_idx..];
                        return Some(ScanToken::Mess(mess));
                    }
                }
            }
        }

        if is_mess(remaining) {
            self.stage = ScanStage::Done;
            let mess = if let Some(start) = self.mess_start {
                &self.v[start..]
            } else {
                remaining
            };
            if !mess.is_empty() {
                return Some(ScanToken::Mess(mess));
            }
        }

        if self.stage == ScanStage::Prerel {
            if let Some((prerel, _build)) = remaining.split_once('+') {
                self.stage = ScanStage::Build;
                self.idx += 1 + prerel.len();
                return Some(ScanToken::Prerel(prerel));
            } else {
                self.stage = ScanStage::Done;
                return Some(ScanToken::Prerel(remaining));
            }
        }

        if self.stage == ScanStage::Build {
            self.stage = ScanStage::Done;
            if !remaining.is_empty() {
                return Some(ScanToken::Build(remaining));
            }
        }

        if self.stage == ScanStage::Mess {
            self.stage = ScanStage::Done;
            let mess = if let Some(start) = self.mess_start {
                &self.v[start..]
            } else {
                remaining
            };
            if !mess.is_empty() {
                return Some(ScanToken::Mess(mess));
            }
        }

        None
    }
}

impl<'a> ScanIterator<'a> {
    fn new(v: &'a str) -> ScanIterator<'a> {
        Self {
            v,
            idx: 0,
            stage: ScanStage::Nums,
            mess_start: None,
            consumed: 0,
            char_count: v.chars().count(),
        }
    }
}

pub fn scan_it<'a>(v: &'a str) -> ScanIterator<'a> {

    let v = v.strip_prefix("v")
             .or_else(|| v.strip_prefix("V"))
             .unwrap_or(v);

    ScanIterator::new(v)
}

// ---/ScanIterator --------------------------------------------------

// --- VersionRef ----------------------------------------------------

#[derive(Debug)]
pub struct VersionRef<'a> {
    pub v: &'a str,
}

impl<'a> PartialEq for VersionRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        matches!(scan_cmp(self.v, other.v), std::cmp::Ordering::Equal)
    }
}
impl<'a> Eq for VersionRef<'a> {}
impl<'a> PartialOrd for VersionRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<'a> Ord for VersionRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {

        #[cfg(feature="semver")]
        {
            if let Ok(left) = semver::Version::parse(self.v) {
                if let Ok(right) = semver::Version::parse(other.v) {
                    return left.cmp(&right);
                }
            }
        }

        scan_cmp(self.v, other.v)
    }
}

impl<'a> VersionRef<'a> {
    pub fn new(v: &'a str) -> Self {
        Self { v }
    }
    pub fn as_str(&self) -> &str {
        self.v
    }
}

impl<'a> std::fmt::Display for VersionRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.v)
    }
}

//impl<'a> AsRef<str> for VersionRef<'a> {
//    fn as_ref(&self) -> &str {
//        self.v
//    }
//}

//impl<'a> std::ops::Deref for VersionRef<'a> {
//    type Target = str;
//    fn deref(&self) -> &Self::Target {
//        self.as_str()
//    }
//}


// ---/VersionRef ----------------------------------------------------

// --- VersionOwned --------------------------------------------------

pub struct VersionOwned(pub String);

impl PartialEq for VersionOwned {
    fn eq(&self, other: &Self) -> bool {
        let left = VersionRef::new(&self.0);
        let right = VersionRef::new(&other.0);
        //left.parse().eq(&right.parse())
        left.eq(&right)
    }
}
impl Eq for VersionOwned {}
impl PartialOrd for VersionOwned {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for VersionOwned {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left = VersionRef::new(&self.0);
        let right = VersionRef::new(&other.0);
        //left.parse().cmp(&right.parse())
        left.cmp(&right)
    }
}

impl VersionOwned {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

// ---/VersionOwned --------------------------------------------------

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn mess() {

        // note:  this is a mess if it is a full version string,
        // it is not identified as a mess by is_mess() because this could be a
        // valid prerelease or build part
        assert!(!is_mess("a.b.c"));

        assert!(is_mess(".1.2"));
        assert!(is_mess("1..2"));
        assert!(is_mess("1=2"));
        assert!(is_mess("1.2.3-4:5"));

        assert!(!is_mess("1"));
        assert!(!is_mess("1.2"));
        assert!(!is_mess("1.2.3"));
        assert!(!is_mess("1.2.3-4"));
        assert!(!is_mess("1.2.3-4.5"));
        assert!(!is_mess("1.2.3+6"));
        assert!(!is_mess("1.2.3+6+7"));
        assert!(!is_mess("1.2.3-4.5+6+7"));
    }

    #[test]
    fn one() {
        let v1 = VersionRef::new("v1.2.3.4.5-7-2-gc3d4+extra-info");
        let v2 = VersionRef::new( "1.2.3.4.5-7-10-ga1b2+extra-info");
        assert!(v1 < v2);
    }

    fn assert_order(versions: &[&str]) {

        for (i, s1) in versions.iter().enumerate() {
            for s2 in versions.iter().skip(i+1) {

                println!("s1: {s1}");
                println!("s2: {s2}");

                assert!(matches!(scan_cmp(s1, s2), std::cmp::Ordering::Less));
                assert!(matches!(scan_cmp(s2, s1), std::cmp::Ordering::Greater));

                let v1 = VersionRef::new(s1);
                let v2 = VersionRef::new(s2);
                assert!(v1 < v2);
                assert!(v2 > v1);

                let vo1 = VersionOwned(s1.to_string());
                let vo2 = VersionOwned(s2.to_string());
                assert!(vo1 < vo2);
                assert!(vo2 > vo1);

                #[cfg(feature="semver")]
                if let Ok(sv1) = semver::Version::parse(s1) {
                    if let Ok(sv2) = semver::Version::parse(s2) {
                        assert!(sv1 < sv2);
                    }
                }
            }
        }
    }

    #[test]
    fn order() {

        // some of these are semver
        let versions = [

            "a.b.c", // this is a mess only, and is less than all non-mess
            "a.b.z",

            "0",
            "0.0.1",
            "0.1",
            "0.1.0",
            "1",
            "1.0",

            // these are true semver strings,
            // and are ordered according to semver rules
            // see [semver.org]
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.beta",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0",

            // less nums is lesser than more nums
            "1.1",
            "1.1.0",
            "1.1.1",
            "1.1.1.0",
            "1.1.1.0.0",
            "1.1.1.1",
            "1.1.1.1.0",

            // prerel is lesser than non-prerel
            // prerel with build is greater than prerel without build
            // build is greater than non-build
            "1.2.3-1",
            "1.2.3-1+1",
            "1.2.3",
            "1.2.3+1",
            "1.2.3.4-1",
            "1.2.3.4-1+1",
            "1.2.3.4",
            "1.2.3.4+1",

            "2",
            "2.0",
            "2.0.0-1",
            "2.0.0-2",
            "2.0.0-10",
            "2.0.0-20-1",
            "2.0.0-20-1+1",
            "2.0.0-20-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-20-2+10",
            #[cfg(not(feature="semver"))]
            "2.0.0-20-10",
            #[cfg(not(feature="semver"))]
            "2.0.0-20-10+2",

            // ascii ordering
            "2.0.0-30-A-1",
            "2.0.0-30-A-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-30-A-10",
            "2.0.0-30-B-1",
            "2.0.0-30-B-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-30-B-10",
            "2.0.0-30-a-1",
            "2.0.0-30-a-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-30-a-10",
            "2.0.0-30-b-1",
            "2.0.0-30-b-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-30-b-10",
            "2.0.0-A-1",
            "2.0.0-A-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-A-10",
            "2.0.0-B-1",
            "2.0.0-B-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-B-10",
            "2.0.0-a-1",
            "2.0.0-a-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-a-10",
            "2.0.0-b-1",
            "2.0.0-b-2",
            #[cfg(not(feature="semver"))]
            "2.0.0-b-10",

            "2.0.0",
            "2.0.0+1",
            "2.0.0+2",
            "2.0.0+2-1",
            "2.0.0+2-2",
            #[cfg(not(feature="semver"))]
            "2.0.0+2-10",
            "2.0.0.0",
            "2.1-12-1+1",
            "2.1-12-1+2",
            "2.1-12-1+10",
            "2.1-12-1-1",
            "2.1-12-1-2",
            "2.1-12-1-10",
            "3.1.2.3-1.1",
            "3.1.2.3-1.2",
            "3.1.2.3-1.10",

            // less parts is lesser (more parts is greater)
            "3.2.1.0-1",
            "3.2.1.0-1-2",
            "3.2.1.0-1-2-3",
            "3.2.1.0-1-2-3-4",
            "3.2.1.0",
            "3.2.1.0+1",
            "3.2.1.0+1-2",
            "3.2.1.0+1-2-3",
            "3.2.1.0+1-2-3-4",

            // less parts is lesser (more parts is greater)
            // also inside of groups split by -
            "3.2.1.1+1-10",
            "3.2.1.1+1.2-10",
            "3.2.1.1+1.2.3-10",

            // python versions including prereleases
            "3.12.0",
            "3.13.0",
            "3.13.1",
            "3.14.0a1",
            "3.14.0a2",
            "3.14.0b1",
            "3.14.0b2",
            "3.14.0rc1",
            "3.14.0",
            "3.14.1",

            // non-numeric parts are ordered by ascii
            "4.1.1-BRANCH",
            "4.1.1-MAIN",
            "4.1.1-TRIAL",
            "4.1.1-branch",
            "4.1.1-main",
            "4.1.1-trial",
            "4.1.1.1-BRANCH",
            "4.1.1.1-MAIN",
            "4.1.1.1-TRIAL",
            "4.1.1.1-branch",
            "4.1.1.1-main",
            "4.1.1.1-trial",

            // non-numeric parts are also ordered by ascii in prerel
            "4.2.2.2-1.2.BRANCH",
            "4.2.2.2-1.2.MAIN",
            "4.2.2.2-1.2.TRIAL",
            "4.2.2.2-1.2.branch",
            "4.2.2.2-1.2.main",
            "4.2.2.2-1.2.trial",

            // non-numeric parts are also ordered by ascii in build
            "4.3.3.3+1.2.BRANCH",
            "4.3.3.3+1.2.MAIN",
            "4.3.3.3+1.2.TRIAL",
            "4.3.3.3+1.2.branch",
            "4.3.3.3+1.2.main",
            "4.3.3.3+1.2.trial",

            // git describe
            "5.1.1.1-6-ga1b2",
            "5.1.1.1-7-ga1b2",
            "5.1.1.1-8-ga1b2",

            // similar, with more info inserted
            "5.2.1.1-6-1-ga1b2+branch-name",
            "5.2.1.1-7-1-ga1b2+branch-name",
            "5.2.1.1-7-2-ga1b2+branch-name",
            "5.2.1.1-7-10-ga1b2+branch-name",
            "5.2.1.1-8-1-ga1b2+branch-name",

            // mess - messy versions are always lesser than non-messy
            "6.0",

            #[cfg(feature="eager-mess")]
            "6.1.2a", // 2a is messy, this is 6.1 and "2a"
            #[cfg(feature="eager-mess")]
            "6.1.2a-2", // a mess does not have prerel or build
            #[cfg(feature="eager-mess")]
            "6.1.2a-10",
            "6.1",
            "6.1.2:5-1",
            "6.1.2:5-2",
            "6.1.2:5-10",
            "6.1.2:3a",
            "6.1.2:4a",
            #[cfg(feature="lazy-mess")]
            "6.1.2a", // 2a is messy, this is 6.1 and "2a"
            #[cfg(feature="lazy-mess")]
            "6.1.2a-2", // a mess does not have prerel or build
            #[cfg(feature="lazy-mess")]
            "6.1.2a-10",
            "6.1.2",
            "6.2++0-rc1",
            "6.003.1a+2025.06.20+4",
            "6.4:01.2",
            "6.4:1.2",

            "7.0.0",
            "7.0.1+1::a",
            "7.0.1-1::b",
            #[cfg(feature="eager-mess")]
            "7.0.1.1a",    // ((7,0,1), ".1a")
            "7.0.1",
            #[cfg(feature="lazy-mess")]
            "7.0.1.1a",    // ((7,0,1,1), "a")

            "7.1.1+1::a",  // ((7,1,1), "+1::a")
            "7.01.1-1::a", // ((7.1.1), "-1::a")

            // numeric parts separated by - are still compared numerically
            "7.1.2-1-1",
            "7.1.2-1-2",
            #[cfg(not(feature="semver"))]
            "7.1.2-1-10",
            "7.1.2-1-20",
            "7.1.2-1-etc-1",
            "7.1.2-1-etc-2",
            #[cfg(not(feature="semver"))]
            "7.1.2-1-etc-10",
            "7.1.2-1-etc-20",

            // prerel is lesser than non-prerel
            // build is greater than non-build
            "8.1.2-rc1",
            "8.1.2",
            "8.1.2+etc",
            "8.1.2.3-rc1",
            "8.1.2.3",
            "8.1.2.3+etc",

            // leading 0s in a number are usually ignored, but break ties and are lesser
            "9.1.0",
            "9.2.0",
            "9.0010.0",
            "9.010.0",
            "9.10.0",
            "9.11.0",

            "10.1.1",
            "v10.1.2", // leading v is stripped
            "V10.1.3",
            "10.1.4",
            "10.1.5-1%3",

            "11.a.b.a",
            "11.a.b.z",

            // mess < prerel < normal < build
            "12.1.2::1.2",
            "12.1.2::1.10",
            "12.1.2-1.2",
            "12.1.2-1.10",
            "12.1.2",
            "12.1.2+1.2",
            "12.1.2+1.10",

            "13.1:+1.0002.3", // ((13, 1), (":+", 1, ".", 0002, ".", 3))
            "13.1:+1.002.3",  // ((13, 1), (":+", 1, ".",  002, ".", 3))
            "13.1:+1.02.3",   // ((13, 1), (":+", 1, ".",   02, ".", 3))
            "13.1:-1.02.3",   // ((13, 1), (":-", 1, ".",   02, ".", 3))
            "13.1:+1.2.3",    // ((13, 1), (":+", 1, ".",    2, ".", 3))
            "13.1:-1.2.3",    // ((13, 1), (":-", 1, ".",    2, ".", 3))

            // these all eval to 14.1-1-1.2 but tiebreakers are based on leading zeros
            "14.1-01-1.2",    // ((14, 1), (01, (1,    2)))
            "14.1-1-1.0002",  // ((14, 1), ( 1, (1, 0002)))
            "14.1-1-1.002",   // ((14, 1), ( 1, (1,  002)))
            "14.1-1-1.02",    // ((14, 1), ( 1, (1,   02)))
        ];

        assert_order(&versions);
    }

    #[cfg(feature="dict")]
    #[test]
    fn dict_order() {


        let versions = [
            "15.0-trial-1",
            "15.0-trial-2",
            "15.0-trial-10",
            "15.0-trial-22",
            "15.0-trial-210",

            "15.0-trial1",
            "15.0-trial2",
            "15.0-trial10",
            "15.0-trial22",
            "15.0-trial210",
        ];

        assert_order(&versions);
    }

    #[cfg(not(feature="dict"))]
    #[test]
    fn non_dict_order() {

        // numbers should be split from strings if they need to be ordered:
        // notice these are not in numeric order, they are in ascii order

        let versions = [
            "15.0-trial-1",
            "15.0-trial-2",
            "15.0-trial-10",
            "15.0-trial-22",
            "15.0-trial-210",

            "15.0-trial1",
            "15.0-trial10",
            "15.0-trial2",
            "15.0-trial210",
            "15.0-trial22",
        ];

        assert_order(&versions);
    }
}


