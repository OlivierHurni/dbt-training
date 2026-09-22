{#
  Net amount of an order line: quantity * unit price, minus the discount (in percent).
  Usage: {{ net_amount('quantity', 'unit_price', 'discount_pct') }}
#}
{% macro net_amount(quantity, unit_price, discount_pct, precision=2) -%}
    round({{ quantity }} * {{ unit_price }} * (1 - {{ discount_pct }} / 100.0), {{ precision }})
{%- endmacro %}
