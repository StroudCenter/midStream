from django.conf.urls import patterns, url
from SensorInputs import views

#Create mappings using the required tuple urlpatterns
# The 'name' in the pattern is what you use when you call a {% url 'name' %} function.
urlpatterns = patterns('',
                       url(r'^$',views.index,name='index'),
                       url(r'about/',views.about,name='about'),
                       url(r'input/',views.streaming_input,name='input'),
                      )



