# Synthetic test fixtures

All files here are hand-written synthetic data. None of it is real NYC data.
The files are byte-exact (`.gitattributes` disables line-ending conversion), because
the tests check the null vs. empty-string distinction, embedded newlines, and similar
details down to the byte.

| File | Purpose |
| --- | --- |
| `edge_cases.csv` | 9 records: a normal record, unquoted-empty (null) and quoted-empty (`""`) fields, trailing whitespace, leading-zero identifiers (`0000123`, `0001`, `007`), a quoted comma, an embedded newline, Unicode, invalid-looking dates, escaped quotes, one exact duplicate row, and one all-null row. |
| `extra_column.csv` | Header with an additional column the contract does not expect. |
| `header_only.csv` | Header with zero data records (an empty dataset). |
| `missing_column.csv` | Header missing an expected column. |
| `malformed_short_row.csv` | Record with too few fields (Polars alone would silently pad it with nulls). |
| `malformed_long_row.csv` | Record with too many fields. |
| `malformed_unterminated_quote.csv` | A quoted field that is never closed. |
| `malformed_blank_line.csv` | A blank line between records (Polars alone would create an all-null row). |
| `duplicate_header.csv` | Two columns with the same name (Polars alone would silently rename one). |

Invalid UTF-8, CRLF line endings, a UTF-8 BOM, and zero-byte files are built inside the
tests from byte literals, so the exact bytes are visible in the test code.
