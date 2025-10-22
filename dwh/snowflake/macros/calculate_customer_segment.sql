{% macro calculate_customer_segment(total_orders, total_revenue) %}
    case
        when {{ total_orders }} >= 10 and {{ total_revenue }} >= 5000 then 'VIP'
        when {{ total_orders }} >= 5 and {{ total_revenue }} >= 2000 then 'Premium'
        when {{ total_orders }} >= 2 and {{ total_revenue }} >= 500 then 'Regular'
        else 'New'
    end
{% endmacro %}
