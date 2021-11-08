from ckan.plugins import toolkit as toolkit
from flask import Blueprint

datitrentinoit = Blueprint('datitrentinoit', __name__)

static_pages = ['faq', 'acknowledgements', 'legal_notes', 'privacy']

for page_name in static_pages:
    def get_action(name):
        def action(self):
            return toolkit.render('pages/{0}.html'.format(name))

        return action


    action = get_action(page_name)
    action.__name__ = page_name

    page_slug = page_name.replace('_', '-')
    #             m.connect(page_name, '/' + page_slug, action=page_name)
    datitrentinoit.add_url_rule('/' + page_slug, page_name, view_func=action, )
