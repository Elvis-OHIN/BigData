from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import DenseVector

# Initialisation de Flask et de Flask-RESTX
app = Flask(__name__)
api = Api(app, version="1.0", title="API de Prédiction d'Objets Perdus",
          description="API permettant de prédire la probabilité de restitution d'un objet perdu.",
          doc="/swagger")

# Namespace pour l'organisation
ns = api.namespace('predict', description='Opérations de prédiction')

# Modèle d'entrée pour Swagger
prediction_input = api.model('PredictionInput', {
    'date': fields.String(required=True, description='Date de perte de l\'objet', example='2024-03-01'),
    'gc_obo_gare_origine_r_name': fields.String(required=True, description='Nom de la gare d\'origine', example='Gare du Nord'),
    'gc_obo_nature_c': fields.String(required=True, description='Nature de l\'objet perdu', example='Portefeuille noir')
})

# Initialisation de Spark
spark = SparkSession.builder.appName("RiskScoring_API").getOrCreate()

# Chargement du modèle PySpark
model = PipelineModel.load("../risk_scoring_model")

# Fonction de conversion des DenseVectors en listes
def convert_densevector_to_list(probability):
    if isinstance(probability, DenseVector):
        return probability.toArray().tolist()  # Conversion en liste
    return probability

# Endpoint API pour les prédictions
@ns.route('/')
class Prediction(Resource):
    @ns.expect([prediction_input])  # Swagger attend une liste d'objets JSON
    def post(self):
        """
        Prédire la probabilité de restitution d'un objet perdu.
        """
        try:
            data = request.get_json()
            df = spark.createDataFrame(data)
            predictions = model.transform(df)

            # Transformation des résultats pour JSON
            results = predictions.select("gc_obo_nature_c", "gc_obo_gare_origine_r_name", "probability").toPandas()

            # Conversion des DenseVectors en listes Python
            results["probability"] = results["probability"].apply(convert_densevector_to_list)

            # Retour de la réponse JSON
            response = results.to_dict(orient="records")
            return jsonify(response)

        except Exception as e:
            return jsonify({"error": str(e)})

# Lancement de l'API
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
