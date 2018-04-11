# Task params
{{ property_name }} = {
    {% for key, value in property_dict.iteritems() %}'{{key}}': '{{value}}',
    {% endfor %}
}
