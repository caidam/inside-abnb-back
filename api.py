# app.py
from flask import Flask, Response
from flask_cors import CORS
from flask_restful import Resource, Api
import queries_service
from decouple import config
import logging

app = Flask(__name__)
CORS(app)
api = Api(app)

# RESOURCE CLASSES
class HelloWorld(Resource):
    def get(self):
        return {'hello': 'This is a Flask API. Changed !'}

# class Get5Listings(Resource):
#     def get(self):
#         json_data = queries_service.get_5_listings()
#         return Response(response=json_data, status=200, content_type='application/json')
    
class Get5Listings(Resource):
    def get(self):
        try:
            logging.info("Get5Listings API called")
            json_data = queries_service.get_5_listings()
            logging.info("Successfully retrieved data from Motherduck database")
            return Response(response=json_data, status=200, content_type='application/json')
        except Exception as e:
            logging.error(f"Error in Get5Listings: {e}")
            return Response(response='{"error": "An error occurred"}', status=500, content_type='application/json')

class GetCities(Resource):
    def get(self):
        json_data = queries_service.get_cities()
        return Response(response=json_data, status=200, content_type='application/json')

class GetCity(Resource):
    def get(self, city='Paris'):
        json_data = queries_service.get_city(city)
        return Response(response=json_data, status=200, content_type='application/json')
    
class GetMarkers(Resource):
    def get(self, city='Paris', neighbourhood=None):
        json_data = queries_service.get_markers(city, neighbourhood)
        return Response(response=json_data, status=200, content_type='application/json')

class GetNeighbourhoods(Resource):
    def get(self, city='Paris'):
        json_data = queries_service.get_neigbourhoods(city)
        return Response(response=json_data, status=200, content_type='application/json')

class GetCityKpis(Resource):
    def get(self, city='Paris', neighbourhood=None):
        json_data = queries_service.get_city_kpis(city, neighbourhood)
        return Response(response=json_data, status=200, content_type='application/json')

class GetTopHosts(Resource):
    def get(self, city='Paris', neighbourhood=None):
        json_data = queries_service.get_top_hosts(city, neighbourhood)
        return Response(response=json_data, status=200, content_type='application/json')

# class GetTopSkills(Resource):
#     def get(self, job_search=None):
#         json_data = queries_service.get_top_skills_data(job_search)
#         return Response(response=json_data, status=200, content_type='application/json')

# class GetTop5Jobs(Resource):
#     def get(self):
#         json_data = queries_service.get_top_5_jobs()
#         return Response(response=json_data, status=200, content_type='application/json')

# ROUTES
api.add_resource(HelloWorld, '/')
api.add_resource(Get5Listings, '/5_listings')
api.add_resource(GetCities, '/cities')
api.add_resource(GetCity, '/city/', '/city/<string:city>')
api.add_resource(GetMarkers, '/markers/', '/markers/<string:city>', '/markers/<string:city>/<string:neighbourhood>')
api.add_resource(GetNeighbourhoods, '/neighbourhoods/', '/neighbourhoods/<string:city>')
api.add_resource(GetCityKpis, '/city_kpis/', '/city_kpis/<string:city>', '/city_kpis/<string:city>/<string:neighbourhood>')
api.add_resource(GetTopHosts, '/top_hosts/', '/top_hosts/<string:city>', '/top_hosts/<string:city>/<string:neighbourhood>')
# api.add_resource(GetTopSkills, '/top_skills', '/top_skills/<string:job_search>')
# api.add_resource(GetTop5Jobs, '/top_5_jobs')

if __name__ == '__main__':

    # app.run(host='0.0.0.0', port=5000, debug=config('DEBUG'))
    app.run(host='0.0.0.0', port=80, debug=config('DEBUG')) # test EC2