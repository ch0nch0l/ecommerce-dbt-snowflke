-- ─────────────────────────────────────────────────────────────────────────────
-- MACRO: cents_to_dollars
-- Converts a cents integer column to a dollar decimal.
-- Usage: {{ cents_to_dollars('price_cents') }}
-- ─────────────────────────────────────────────────────────────────────────────
{% macro cents_to_dollars(column_name, precision=2) %}
    round(cast({{ column_name }} as numeric) / 100.0, {{ precision }})
{% endmacro %}


-- ─────────────────────────────────────────────────────────────────────────────
-- MACRO: safe_divide
-- Division that returns NULL instead of dividing by zero.
-- Usage: {{ safe_divide('revenue', 'orders') }}
-- ─────────────────────────────────────────────────────────────────────────────
{% macro safe_divide(numerator, denominator, precision=4) %}
    round(
        {{ numerator }} / nullif({{ denominator }}, 0),
        {{ precision }}
    )
{% endmacro %}


-- ─────────────────────────────────────────────────────────────────────────────
-- MACRO: date_to_fiscal_quarter
-- Maps a date to a fiscal quarter label (e.g. 'FY2024-Q1').
-- Assumes fiscal year starts in January (adjust offset for other calendars).
-- Usage: {{ date_to_fiscal_quarter('ordered_at') }}
-- ─────────────────────────────────────────────────────────────────────────────
{% macro date_to_fiscal_quarter(date_column, fiscal_year_start_month=1) %}
    'FY' || year({{ date_column }}) || '-Q' ||
    ceil(
        (month({{ date_column }})
         - {{ fiscal_year_start_month }} + 12) % 12 / 3.0 + 1
    )::int
{% endmacro %}


-- ─────────────────────────────────────────────────────────────────────────────
-- MACRO: is_weekday
-- Returns TRUE if the date falls on a weekday (Mon–Fri).
-- Usage: {{ is_weekday('ordered_at') }}
-- ─────────────────────────────────────────────────────────────────────────────
{% macro is_weekday(date_column) %}
    dayofweek({{ date_column }}) not in (1, 7)  -- 1=Sunday, 7=Saturday
{% endmacro %}
