import os
import json
from flask import Flask, request, jsonify
from pymongo import MongoClient
from google.cloud import secretmanager
import logging

app = Flask(__name__)

# Initialize Secret Manager client
client = secretmanager.SecretManagerServiceClient()

def get_secret(secret_name):
    """Retrieve secret from Google Secret Manager"""
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Error accessing secret {secret_name}: {str(e)}")
        return None

def get_mongodb_client():
    """Get MongoDB client using connection string from secrets"""
    connection_string = get_secret("mongodb-connection-string")
    if not connection_string:
        raise Exception("MongoDB connection string not found in secrets")
    return MongoClient(connection_string)

@app.route('/find_movies', methods=['POST'])
def find_movies():
    """Find movies based on natural language query"""
    try:
        data = request.get_json()
        query = data.get('query', '')
        limit = data.get('limit', 10)
        
        if not query:
            return jsonify({'error': 'Query parameter is required'}), 400
        
        # Connect to MongoDB
        mongodb_client = get_mongodb_client()
        db = mongodb_client.sample_mflix
        collection = db.movies
        
        # Use Atlas Search for natural language queries
        search_pipeline = [
            {
                "$search": {
                    "index": "movies_search_index",
                    "text": {
                        "query": query,
                        "path": {"wildcard": "*"}
                    }
                }
            },
            {"$limit": limit},
            {
                "$project": {
                    "title": 1,
                    "year": 1,
                    "plot": 1,
                    "genres": 1,
                    "cast": 1,
                    "directors": 1,
                    "rated": 1,
                    "runtime": 1,
                    "score": {"$meta": "searchScore"}
                }
            }
        ]
        
        results = list(collection.aggregate(search_pipeline))
        
        # Convert ObjectId to string for JSON serialization
        for result in results:
            if '_id' in result:
                result['_id'] = str(result['_id'])
        
        return jsonify({
            'success': True,
            'query': query,
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        logging.error(f"Error in find_movies: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/find_movie_by_title', methods=['POST'])
def find_movie_by_title():
    """Find a specific movie by title"""
    try:
        data = request.get_json()
        title = data.get('title', '')
        
        if not title:
            return jsonify({'error': 'Title parameter is required'}), 400
        
        # Connect to MongoDB
        mongodb_client = get_mongodb_client()
        db = mongodb_client.sample_mflix
        collection = db.movies
        
        # Find movie by title (case insensitive)
        movie = collection.find_one(
            {"title": {"$regex": title, "$options": "i"}},
            {
                "title": 1,
                "year": 1,
                "plot": 1,
                "genres": 1,
                "cast": 1,
                "directors": 1,
                "rated": 1,
                "runtime": 1
            }
        )
        
        if movie:
            movie['_id'] = str(movie['_id'])
            return jsonify({
                'success': True,
                'movie': movie
            })
        else:
            return jsonify({
                'success': False,
                'message': f'Movie with title "{title}" not found'
            }), 404
            
    except Exception as e:
        logging.error(f"Error in find_movie_by_title: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'mongodb-vertex-api'})

def mongodb_crud(request):
    """Main entry point for Cloud Function"""
    with app.test_request_context(request.url, method=request.method, 
                                  data=request.data, headers=request.headers):
        try:
            return app.full_dispatch_request()
        except Exception as e:
            logging.error(f"Error in mongodb_crud: {str(e)}")
            return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
