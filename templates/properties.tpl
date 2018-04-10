# Task params
{{ property_name }} = {
    {% for key, value in property_dict.iteritems() %}'{{key}}': '{{ textwrap_func(key, value, 8)}}',
    {% endfor %}
}
