{%  macro percent_diff_values(val1,val2) %}
    (({{val1}} - {{val2}})/{{val1}}) * 100
{% endmacro %}