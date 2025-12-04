# -*- coding: utf-8 -*-

from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.core import (QgsProcessing,
                       QgsFeatureSink,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterVectorLayer,
                       QgsProcessingParameterNumber,
                       QgsProcessingParameterFeatureSink,
                       QgsProcessingParameterField,
                       QgsFeature,
                       QgsGeometry,
                       QgsPointXY,
                       QgsSpatialIndex,
                       QgsField,
                       QgsVectorLayer,
                       QgsProject,
                       QgsProcessingUtils,
                       QgsFeatureRequest,
                       QgsProcessingParameterBoolean,
                       QgsProcessingException,
                       QgsCoordinateTransform)
from qgis import processing

class TransportAccessibilityIsochrones(QgsProcessingAlgorithm):
    INPUT_STOPS_IN = 'INPUT_STOPS_IN'
    INPUT_STOPS_OUT = 'INPUT_STOPS_OUT'
    INPUT_GRAPH = 'INPUT_GRAPH'
    INPUT_CONTOURS = 'INPUT_CONTOURS'
    CONTOUR_FIELD = 'CONTOUR_FIELD'
    INPUT_BUILDINGS = 'INPUT_BUILDINGS'
    POPULATION_FIELD = 'POPULATION_FIELD'
    MAX_COST = 'MAX_COST'
    OUTPUT_LAYER_IN = 'OUTPUT_LAYER_IN'
    OUTPUT_LAYER_OUT = 'OUTPUT_LAYER_OUT'
    OUTPUT_INTERSECTION = 'OUTPUT_INTERSECTION'
    USE_RELIEF = 'USE_RELIEF'

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return TransportAccessibilityIsochrones()

    def name(self):
        return 'transport_accessibility_hackathon_polygon_v2'

    def displayName(self):
        return self.tr('Хакатон: Изохроны (Сплошные Полигоны + Рельеф)')

    def group(self):
        return self.tr('Хакатон Инструменты')

    def groupId(self):
        return 'hackathon'

    def initAlgorithm(self, config=None):
        # остановки для входа в район
        self.addParameter(QgsProcessingParameterVectorLayer(
            self.INPUT_STOPS_IN, self.tr('Остановки (Вход в район)'), [QgsProcessing.TypeVectorPoint]))
        
        # остановки для выезда из района
        self.addParameter(QgsProcessingParameterVectorLayer(
            self.INPUT_STOPS_OUT, self.tr('Остановки (Выезд из района)'), [QgsProcessing.TypeVectorPoint]))

        # пешеходный граф дорог
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_GRAPH, self.tr('Пешеходный граф (линии)'), [QgsProcessing.TypeVectorLine]))

        # изолинии высот для учета рельефа
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_CONTOURS, self.tr('Изолинии высот'), [QgsProcessing.TypeVectorLine]))
        
        # поле с высотой в изолиниях
        self.addParameter(QgsProcessingParameterField(
            self.CONTOUR_FIELD, self.tr('Поле высоты (в изолиниях)'), parentLayerParameterName=self.INPUT_CONTOURS, type=QgsProcessingParameterField.Numeric))

        # здания с населением
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_BUILDINGS, self.tr('Здания (население)'), [QgsProcessing.TypeVectorPolygon, QgsProcessing.TypeVectorPoint]))
            
        # поле с количеством жителей в зданиях
        self.addParameter(QgsProcessingParameterField(
            self.POPULATION_FIELD, self.tr('Поле кол-ва жильцов'), parentLayerParameterName=self.INPUT_BUILDINGS, type=QgsProcessingParameterField.Numeric))

        # лимит доступности в метрах
        self.addParameter(QgsProcessingParameterNumber(
            self.MAX_COST, self.tr('Лимит доступности (условные метры с учетом рельефа)'), type=QgsProcessingParameterNumber.Double, defaultValue=500.0))

        # флаг учета рельефа
        self.addParameter(QgsProcessingParameterBoolean(
            self.USE_RELIEF, self.tr('Учитывать рельеф'), defaultValue=True))

        # выходные слои
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUTPUT_LAYER_IN, self.tr('Зона: Доступность В РАЙОН')))
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUTPUT_LAYER_OUT, self.tr('Зона: Доступность ИЗ РАЙОНА')))
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUTPUT_INTERSECTION, self.tr('Зона: ПОЛНАЯ ДОСТУПНОСТЬ')))

    def processAlgorithm(self, parameters, context, feedback):
        
        # получаем параметры
        use_relief = self.parameterAsBoolean(parameters, self.USE_RELIEF, context)
        max_cost = self.parameterAsDouble(parameters, self.MAX_COST, context)
        
        # вспомогательная функция для получения слоя
        def get_layer_obj(layer_id_or_obj):
            if isinstance(layer_id_or_obj, str):
                lyr = context.temporaryLayerStore().mapLayer(layer_id_or_obj)
                if lyr: return lyr
                lyr = QgsProject.instance().mapLayer(layer_id_or_obj)
                if lyr: return lyr
                return QgsProcessingUtils.mapLayerFromString(layer_id_or_obj, context)
            return layer_id_or_obj

        # подготовка графа
        raw_graph_source = self.parameterAsVectorLayer(parameters, self.INPUT_GRAPH, context)
        
        if use_relief:
            feedback.setProgressText("этап 1: сегментация графа (нарезка по 30м)...")
            split_graph_dict = processing.run("native:splitlinesbylength", {
                'INPUT': raw_graph_source,
                'LENGTH': 30, 
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)
            working_graph_layer = get_layer_obj(split_graph_dict['OUTPUT']) 
        else:
            feedback.setProgressText("этап 1: подготовка графа (без нарезки)...")
            working_graph_layer = raw_graph_source

        if not working_graph_layer:
            raise QgsProcessingException("ошибка: не удалось создать рабочий граф.")

        # настройка функции получения высоты
        get_z = lambda p: 0
        
        if use_relief:
            feedback.setProgressText("подготовка данных рельефа...")
            source_contours = self.parameterAsSource(parameters, self.INPUT_CONTOURS, context)
            contour_field = self.parameterAsString(parameters, self.CONTOUR_FIELD, context)
            
            # строим пространственный индекс изолиний
            contour_index = QgsSpatialIndex(source_contours.getFeatures(), feedback)
            # кэшируем значения высот
            contour_data = {f.id(): f[contour_field] for f in source_contours.getFeatures()}
            
            # трансформация координат между системами
            crs_graph = working_graph_layer.sourceCrs()
            crs_contours = source_contours.sourceCrs()
            
            if not crs_graph.isValid() or not crs_contours.isValid():
                feedback.reportError("внимание: неверная система координат у графа или изолиний!")

            transform_to_contours = QgsCoordinateTransform(crs_graph, crs_contours, context.project())
            
            def get_z_impl(point):
                try:
                    # трансформируем точку в crs изолиний
                    pt_tr = transform_to_contours.transform(point)
                    # ищем ближайшую изолинию
                    nearest_ids = contour_index.nearestNeighbor(pt_tr, 1)
                    if not nearest_ids: return 0
                    # берем значение высоты
                    val = contour_data.get(nearest_ids[0], 0)
                    return float(val) if val is not None else 0
                except Exception as e:
                    return 0
            
            get_z = get_z_impl

        # расчет весов ребер графа
        w_crs = working_graph_layer.sourceCrs()
        weighted_graph_layer = QgsVectorLayer(f"LineString?crs={w_crs.authid()}&index=yes", "WeightedGraph", "memory")
        pr = weighted_graph_layer.dataProvider()
        pr.addAttributes([QgsField("weight", QVariant.Double)])
        weighted_graph_layer.updateFields()

        features_to_add = []
        iterator = working_graph_layer.getFeatures()
        total_feats = working_graph_layer.featureCount()
        
        total_penalty_accumulated = 0 
        debug_printed = False

        feedback.setProgressText("этап 2: расчет стоимости прохода (вес ребра)...")
        for i, feat in enumerate(iterator):
            if feedback.isCanceled(): break
            geom = feat.geometry()
            if not geom: continue
            
            length = geom.length()
            final_cost = length 
            
            if use_relief:
                if geom.isMultipart():
                    lines = geom.asMultiPolyline()
                    line = lines[0] if lines else []
                else:
                    line = geom.asPolyline()
                    
                if len(line) >= 2:
                    p1 = QgsPointXY(line[0])
                    p2 = QgsPointXY(line[-1])
                    
                    z1 = get_z(p1)
                    z2 = get_z(p2)
                    
                    delta_h = abs(z1 - z2)
                    
                    if not debug_printed and i > 10 and delta_h > 0:
                        feedback.pushInfo(f"debug: проверка рельефа работает! p1_z={z1}, p2_z={z2}, delta={delta_h}")
                        debug_printed = True

                    # штраф за перепад высот: 5 условных метров за 1 метр перепада
                    slope_penalty = delta_h * 5.0 
                    final_cost += slope_penalty
                    total_penalty_accumulated += slope_penalty

            new_f = QgsFeature()
            new_f.setGeometry(geom)
            new_f.setAttributes([final_cost])
            features_to_add.append(new_f)
            
            if i % 1000 == 0: 
                feedback.setProgress(int(10 + (30 * i / total_feats)))

        if use_relief:
            if total_penalty_accumulated == 0:
                feedback.reportError("!!! ошибка рельефа: общий начисленный штраф равен 0. проверьте: 1) пересекаются ли слои? 2) верна ли проекция?")
            else:
                feedback.pushInfo(f"успех: учтен рельеф. общий штраф: {int(total_penalty_accumulated)} м.")

        pr.addFeatures(features_to_add)
        weighted_graph_layer.updateExtents()
        
        # построение сплошных зон доступности
        feedback.setProgressText("этап 3: построение геометрии изохрон...")
        
        layer_stops_in = self.parameterAsVectorLayer(parameters, self.INPUT_STOPS_IN, context)
        layer_stops_out = self.parameterAsVectorLayer(parameters, self.INPUT_STOPS_OUT, context)

        if not layer_stops_in or not layer_stops_out:
            raise QgsProcessingException("ошибка загрузки слоев остановок.")

        def build_zone_polygon(stops_layer, suffix):
            # построение линий доступности
            lines_dict = processing.run("native:serviceareafromlayer", {
                'INPUT': weighted_graph_layer, 
                'STRATEGY': 0, 
                'VALUE_BOTH': 'weight',
                'DEFAULT_DIRECTION': 2,
                'DEFAULT_VALUE': 0,
                'START_POINTS': stops_layer,
                'TRAVEL_COST': max_cost,
                'INCLUDE_BOUNDS': False,
                'OUTPUT_LINES': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)
            
            res_lines = get_layer_obj(lines_dict['OUTPUT_LINES'])
            if not res_lines or res_lines.featureCount() == 0:
                feedback.reportError(f"не удалось построить маршруты для {suffix}")
                return None

            # буферизация линий
            buffer_dict = processing.run("native:buffer", {
                'INPUT': lines_dict['OUTPUT_LINES'],
                'DISTANCE': 70, 
                'DISSOLVE': True, 
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)
            
            # удаление дырок внутри полигонов
            feedback.setProgressText(f"заливка пустот для зоны {suffix}...")
            filled_dict = processing.run("native:deleteholes", {
                'INPUT': buffer_dict['OUTPUT'],
                'MIN_AREA': 1000000,
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)

            # исправление геометрии
            fixed_dict = processing.run("native:fixgeometries", {
                'INPUT': filled_dict['OUTPUT'],
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)
            
            return get_layer_obj(fixed_dict['OUTPUT'])

        zone_in = build_zone_polygon(layer_stops_in, "вход")
        zone_out = build_zone_polygon(layer_stops_out, "выход")
        
        if not zone_in or not zone_out:
             raise QgsProcessingException("сбой при построении зон.")

        # пересечение зон для получения общей доступности
        feedback.setProgressText("этап 4: пересечение зон...")
        zone_inter_dict = processing.run("native:intersection", {
            'INPUT': zone_in,
            'OVERLAY': zone_out,
            'OUTPUT': 'TEMPORARY_OUTPUT'
        }, context=context, feedback=feedback, is_child_algorithm=True)
        zone_inter = get_layer_obj(zone_inter_dict['OUTPUT'])

        # подсчет населения в зонах
        feedback.setProgressText("этап 5: подсчет населения...")
        pop_source = self.parameterAsSource(parameters, self.INPUT_BUILDINGS, context)
        pop_field = self.parameterAsString(parameters, self.POPULATION_FIELD, context)

        def calc_pop(zone_lyr, name):
            if not zone_lyr: return None
            total_pop = 0
            
            zone_iter = zone_lyr.getFeatures()
            zone_feat = next(zone_iter, None)
            
            if zone_feat:
                zone_geom = zone_feat.geometry()
                
                # используем пространственный индекс для быстрого поиска зданий
                bld_idx = QgsSpatialIndex(pop_source.getFeatures(), feedback)
                candidate_ids = bld_idx.intersects(zone_geom.boundingBox())
                req = QgsFeatureRequest().setFilterFids(candidate_ids)
                
                for bld in pop_source.getFeatures(req):
                    if bld.geometry().intersects(zone_geom):
                        val = bld[pop_field]
                        if val: total_pop += val
            
            feedback.pushInfo(f"--- {name}: {int(total_pop)} чел. ---")
            
            # добавляем поле с рассчитанным населением
            res_dict = processing.run("native:addfieldtoattributestable", {
                'INPUT': zone_lyr,
                'FIELD_NAME': 'calc_pop',
                'FIELD_TYPE': 0, 
                'FIELD_LENGTH': 10,
                'OUTPUT': 'TEMPORARY_OUTPUT' 
            }, context=context, feedback=feedback, is_child_algorithm=True)
            
            res_lyr = get_layer_obj(res_dict['OUTPUT'])
            if res_lyr:
                res_lyr.startEditing()
                for f in res_lyr.getFeatures():
                    res_lyr.changeAttributeValue(f.id(), res_lyr.fields().indexOf('calc_pop'), int(total_pop))
                res_lyr.commitChanges()
            return res_lyr

        final_in = calc_pop(zone_in, "в район")
        final_out = calc_pop(zone_out, "из района")
        final_inter = calc_pop(zone_inter, "пересечение")

        # сохранение результатов
        results = {}
        outputs_map = [
            (final_in, self.OUTPUT_LAYER_IN, "зона_вход"),
            (final_out, self.OUTPUT_LAYER_OUT, "зона_выход"),
            (final_inter, self.OUTPUT_INTERSECTION, "зона_общая")
        ]

        for layer, sink_name, debug_name in outputs_map:
            if layer:
                (sink, dest_id) = self.parameterAsSink(parameters, sink_name, context,
                                                       layer.fields(), layer.wkbType(), layer.sourceCrs())
                if sink:
                    sink.addFeatures(layer.getFeatures(), QgsFeatureSink.FastInsert)
                    results[sink_name] = dest_id
                    
                    # для отладки можно добавить слои на карту
                    stored_lyr = QgsProcessingUtils.mapLayerFromString(dest_id, context)
                    if stored_lyr:
                        stored_lyr.setName(debug_name)

        return results