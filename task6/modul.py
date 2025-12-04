from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingParameterFeatureSource
from qgis.core import QgsProcessingParameterField
from qgis.core import QgsProcessingParameterNumber
from qgis.core import QgsProcessingParameterFeatureSink
import processing

class ParkingDeficitAnalyzer(QgsProcessingAlgorithm):

    INPUT_GRID = 'INPUT_GRID'
    INPUT_PARKING = 'INPUT_PARKING'
    INPUT_BUILDINGS = 'INPUT_BUILDINGS'
    FIELD_CAPACITY = 'FIELD_CAPACITY'
    FIELD_POPULATION = 'FIELD_POPULATION'
    FIELD_RATIO = 'FIELD_RATIO'
    OUTPUT_LAYER = 'OUTPUT_LAYER'


    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT_GRID,
                self.tr('Начальная сетка'),
                [QgsProcessing.Type.VectorPolygon]
            )
        )

        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT_PARKING,
                self.tr('Слой парковочных зон'),
                [QgsProcessing.Type.Vector]
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.FIELD_CAPACITY,
                self.tr('Поле с вместимостью парковочных зон'),
                parentLayerParameterName=self.INPUT_PARKING,
                type=QgsProcessingParameterField.Numeric
            )
        )
        
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT_BUILDINGS,
                self.tr('Слой жилых строений'),
                [QgsProcessing.Type.Vector]
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.FIELD_POPULATION,
                self.tr('Поле количества жителей'),
                parentLayerParameterName=self.INPUT_BUILDINGS,
                type=QgsProcessingParameterField.Numeric
            )
        )

        self.addParameter(
            QgsProcessingParameterNumber(
                self.FIELD_RATIO,
                self.tr('Сколько в среднем человек на 1 место'),
                QgsProcessingParameterNumber.Double,
                defaultValue=2.0
            )
        )

        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT_LAYER,
                self.tr('Финальный слой')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        feedback.pushInfo("--- Запуск анализа дефицита парковок ---")

        parking_field = self.parameterAsFields(parameters, self.FIELD_CAPACITY, self.INPUT_PARKING)[0]
        pop_field = self.parameterAsFields(parameters, self.FIELD_POPULATION, self.INPUT_BUILDINGS)[0]
        ratio = parameters[self.FIELD_RATIO]
        
        parking_sum_field_name = parking_field + '_sum'
        pop_sum_field_name = pop_field + '_sum'


        feedback.pushInfo(f"Расчет суммы парковочных мест ({parking_field})")
        
        parking_sum_result = processing.run("native:joinattributesbylocation", {
            'INPUT': parameters[self.INPUT_GRID],
            'JOIN': parameters[self.INPUT_PARKING],
            'PREDICATE': [0],
            'JOIN_FIELDS': [],
            'SUM_FIELDS': [parking_field],
            'SUMMARY_FIELDS': [2],
            'OUTPUT': 'memory:'
        }, context=context, feedback=feedback, is_child_algorithm=True)
        
        temp_layer_parking = parking_sum_result['OUTPUT']


        feedback.pushInfo(f"Расчет суммы жителей ({pop_field})")
        
        pop_sum_result = processing.run("native:joinattributesbylocation", {
            'INPUT': temp_layer_parking,
            'JOIN': parameters[self.INPUT_BUILDINGS],
            'PREDICATE': [0],
            'JOIN_FIELDS': [],
            'SUM_FIELDS': [pop_field],
            'SUMMARY_FIELDS': [2],
            'OUTPUT': 'memory:'
        }, context=context, feedback=feedback, is_child_algorithm=True)
        
        temp_layer_final = pop_sum_result['OUTPUT']


        feedback.pushInfo("Расчет дефицита")
        
        formula = f' ("{pop_sum_field_name}" / {ratio}) - "{parking_sum_field_name}" '

        deficit_calc_result = processing.run("native:fieldcalculator", {
            'INPUT': temp_layer_final,
            'FIELD_NAME': 'Deficit',
            'FIELD_TYPE': 1, # (Целое число)
            'FIELD_LENGTH': 10,
            'FIELD_PRECISION': 0,
            'FORMULA': formula,
            'OUTPUT': parameters[self.OUTPUT_LAYER]
        }, context=context, feedback=feedback, is_child_algorithm=True)

        return {self.OUTPUT_LAYER: deficit_calc_result['OUTPUT']}

    def tr(self, message):
        return QgsProcessingAlgorithm.tr(self, message)

    def name(self):
        return 'parking_deficit_analyzer'

    def displayName(self):
        return self.tr('Анализ дефицита парковок')

    def group(self):
        return self.tr('Хакатон')

    def groupId(self):
        return 'hackathon'

    def createInstance(self):
        return ParkingDeficitAnalyzer()