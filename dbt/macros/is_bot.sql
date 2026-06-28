{# is_bot(login_column) — ADR-0006 Rule A + Rule C.

   Returns a SQL boolean expression suitable for inclusion in a
   select / where / group by. Centralises bot classification so the
   Bot mart and any other consumer cannot accidentally diverge.

   Rule A: actor login ends with the literal `[bot]` (GitHub
           convention for app-installed accounts).
   Rule C: actor login appears in the curated known_bots seed.

   Usage:
       select {{ is_bot('actor_login') }} as is_bot from ...

   The macro renders inline so the optimiser can push the predicate
   down. The known_bots subquery is a left-anti-style EXISTS — fine
   on the current single-digit-row seed; revisit if it grows past a
   few hundred entries (probably never, see ADR-0006).
#}

{% macro is_bot(login_column) %}
    (
        {{ login_column }} like '%[bot]'
        or {{ login_column }} in (select login from {{ ref('known_bots') }})
    )
{% endmacro %}
