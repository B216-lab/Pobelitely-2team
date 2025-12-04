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
                       QgsCoordinateTransform) # Важный импорт
from qgis import processing

class TransportAccessibilityIsochrones(QgsProcessingAlgorithm):
    # Константы
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
        # 1. Остановки В РАЙОН
        self.addParameter(QgsProcessingParameterVectorLayer(
            self.INPUT_STOPS_IN, self.tr('Остановки (Вход в район)'), [QgsProcessing.TypeVectorPoint]))
        
        # 2. Остановки ИЗ РАЙОНА
        self.addParameter(QgsProcessingParameterVectorLayer(
            self.INPUT_STOPS_OUT, self.tr('Остановки (Выезд из района)'), [QgsProcessing.TypeVectorPoint]))

        # 3. Граф
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_GRAPH, self.tr('Пешеходный граф (линии)'), [QgsProcessing.TypeVectorLine]))

        # 4. Изолинии
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_CONTOURS, self.tr('Изолинии высот'), [QgsProcessing.TypeVectorLine]))
        
        self.addParameter(QgsProcessingParameterField(
            self.CONTOUR_FIELD, self.tr('Поле высоты (в изолиниях)'), parentLayerParameterName=self.INPUT_CONTOURS, type=QgsProcessingParameterField.Numeric))

        # 5. Здания
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_BUILDINGS, self.tr('Здания (население)'), [QgsProcessing.TypeVectorPolygon, QgsProcessing.TypeVectorPoint]))
            
        self.addParameter(QgsProcessingParameterField(
            self.POPULATION_FIELD, self.tr('Поле кол-ва жильцов'), parentLayerParameterName=self.INPUT_BUILDINGS, type=QgsProcessingParameterField.Numeric))

        # 6. Лимит
        self.addParameter(QgsProcessingParameterNumber(
            self.MAX_COST, self.tr('Лимит доступности (условные метры с учетом рельефа)'), type=QgsProcessingParameterNumber.Double, defaultValue=500.0))

        # 7. Флажок использования рельефа
        self.addParameter(QgsProcessingParameterBoolean(
            self.USE_RELIEF, self.tr('Учитывать рельеф'), defaultValue=True))

        # Выходы
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUTPUT_LAYER_IN, self.tr('Зона: Доступность В РАЙОН')))
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUTPUT_LAYER_OUT, self.tr('Зона: Доступность ИЗ РАЙОНА')))
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUTPUT_INTERSECTION, self.tr('Зона: ПОЛНАЯ ДОСТУПНОСТЬ')))

    def processAlgorithm(self, parameters, context, feedback):
        
        # Получаем значение флажка и параметры
        use_relief = self.parameterAsBoolean(parameters, self.USE_RELIEF, context)
        max_cost = self.parameterAsDouble(parameters, self.MAX_COST, context)
        
        # Функция-хелпер для получения слоя
        def get_layer_obj(layer_id_or_obj):
            if isinstance(layer_id_or_obj, str):
                lyr = context.temporaryLayerStore().mapLayer(layer_id_or_obj)
                if lyr: return lyr
                lyr = QgsProject.instance().mapLayer(layer_id_or_obj)
                if lyr: return lyr
                return QgsProcessingUtils.mapLayerFromString(layer_id_or_obj, context)
            return layer_id_or_obj

        # =========================================================================
        # 1. ПОДГОТОВКА ГРАФА
        # =========================================================================
        
        raw_graph_source = self.parameterAsVectorLayer(parameters, self.INPUT_GRAPH, context)
        
        if use_relief:
            feedback.setProgressText("Этап 1: Сегментация графа (нарезка по 30м)...")
            split_graph_dict = processing.run("native:splitlinesbylength", {
                'INPUT': raw_graph_source,
                'LENGTH': 30, 
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)
            working_graph_layer = get_layer_obj(split_graph_dict['OUTPUT']) 
        else:
            feedback.setProgressText("Этап 1: Подготовка графа (без нарезки)...")
            working_graph_layer = raw_graph_source

        if not working_graph_layer:
            raise QgsProcessingException("Ошибка: Не удалось создать рабочий граф.")

        # --- НАСТРОЙКА РЕЛЬЕФА ---
        get_z = lambda p: 0 # По умолчанию высота 0
        
        if use_relief:
            feedback.setProgressText("Подготовка данных рельефа...")
            source_contours = self.parameterAsSource(parameters, self.INPUT_CONTOURS, context)
            contour_field = self.parameterAsString(parameters, self.CONTOUR_FIELD, context)
            
            # Строим индекс
            contour_index = QgsSpatialIndex(source_contours.getFeatures(), feedback)
            # Кэшируем значения высот {id: elevation}
            contour_data = {f.id(): f[contour_field] for f in source_contours.getFeatures()}
            
            # Настройка трансформации CRS (Граф -> Изолинии)
            crs_graph = working_graph_layer.sourceCrs()
            crs_contours = source_contours.sourceCrs()
            
            # Проверяем, валидны ли CRS
            if not crs_graph.isValid() or not crs_contours.isValid():
                feedback.reportError("ВНИМАНИЕ: Неверная система координат у графа или изолиний!")

            transform_to_contours = QgsCoordinateTransform(crs_graph, crs_contours, context.project())
            
            def get_z_impl(point):
                try:
                    # 1. Трансформируем точку в систему координат изолиний
                    pt_tr = transform_to_contours.transform(point)
                    # 2. Ищем ближайшую изолинию
                    nearest_ids = contour_index.nearestNeighbor(pt_tr, 1)
                    if not nearest_ids: return 0
                    # 3. Берем значение
                    val = contour_data.get(nearest_ids[0], 0)
                    return float(val) if val is not None else 0
                except Exception as e:
                    return 0
            
            get_z = get_z_impl

        # --- РАСЧЕТ ВЕСОВ ---
        w_crs = working_graph_layer.sourceCrs()
        weighted_graph_layer = QgsVectorLayer(f"LineString?crs={w_crs.authid()}&index=yes", "WeightedGraph", "memory")
        pr = weighted_graph_layer.dataProvider()
        pr.addAttributes([QgsField("weight", QVariant.Double)])
        weighted_graph_layer.updateFields()

        features_to_add = []
        iterator = working_graph_layer.getFeatures()
        total_feats = working_graph_layer.featureCount()
        
        total_penalty_accumulated = 0 
        debug_printed = False # Чтобы вывести лог только один раз

        feedback.setProgressText("Этап 2: Расчет стоимости прохода (вес ребра)...")
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
                    
                    # --- ЛОГИРОВАНИЕ (ВАЖНО ДЛЯ ПРОВЕРКИ) ---
                    if not debug_printed and i > 10 and delta_h > 0:
                        feedback.pushInfo(f"DEBUG: Проверка рельефа работает! P1_z={z1}, P2_z={z2}, Delta={delta_h}")
                        debug_printed = True
                    # ----------------------------------------

                    # ШТРАФ: 5 условных метров за 1 метр перепада
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
                feedback.reportError("!!! ОШИБКА РЕЛЬЕФА: Общий начисленный штраф равен 0. Проверьте: 1) Пересекаются ли слои? 2) Верна ли проекция?")
            else:
                feedback.pushInfo(f"УСПЕХ: Учтен рельеф. Общий штраф: {int(total_penalty_accumulated)} м.")

        pr.addFeatures(features_to_add)
        weighted_graph_layer.updateExtents()
        
        # =========================================================================
        # 2. ПОСТРОЕНИЕ СПЛОШНЫХ ЗОН
        # =========================================================================
        feedback.setProgressText("Этап 3: Построение геометрии изохрон...")
        
        layer_stops_in = self.parameterAsVectorLayer(parameters, self.INPUT_STOPS_IN, context)
        layer_stops_out = self.parameterAsVectorLayer(parameters, self.INPUT_STOPS_OUT, context)

        if not layer_stops_in or not layer_stops_out:
            raise QgsProcessingException("Ошибка загрузки слоев остановок.")

        def build_zone_polygon(stops_layer, suffix):
            # 1. Service Area (Линии)
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
                feedback.reportError(f"Не удалось построить маршруты для {suffix}")
                return None

            # 2. Буфер (Толстые линии) - Увеличили до 70м
            buffer_dict = processing.run("native:buffer", {
                'INPUT': lines_dict['OUTPUT_LINES'],
                'DISTANCE': 70, 
                'DISSOLVE': True, 
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)
            
            # 3. Удаление дырок (Превращение бубликов в сплошные блины)
            # Это решает проблему "Векторов вдоль дорог"
            feedback.setProgressText(f"Заливка пустот для зоны {suffix}...")
            filled_dict = processing.run("native:deleteholes", {
                'INPUT': buffer_dict['OUTPUT'],
                'MIN_AREA': 1000000, # Удаляем любые дырки меньше 1 кв.км (практически все внутри)
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)

            # 4. Исправление геометрии (на всякий случай)
            fixed_dict = processing.run("native:fixgeometries", {
                'INPUT': filled_dict['OUTPUT'],
                'OUTPUT': 'TEMPORARY_OUTPUT'
            }, context=context, feedback=feedback, is_child_algorithm=True)
            
            return get_layer_obj(fixed_dict['OUTPUT'])

        zone_in = build_zone_polygon(layer_stops_in, "ВХОД")
        zone_out = build_zone_polygon(layer_stops_out, "ВЫХОД")
        
        if not zone_in or not zone_out:
             raise QgsProcessingException("Сбой при построении зон.")

        # =========================================================================
        # 3. ПЕРЕСЕЧЕНИЕ И НАСЕЛЕНИЕ
        # =========================================================================
        
        feedback.setProgressText("Этап 4: Пересечение зон...")
        zone_inter_dict = processing.run("native:intersection", {
            'INPUT': zone_in,
            'OVERLAY': zone_out,
            'OUTPUT': 'TEMPORARY_OUTPUT'
        }, context=context, feedback=feedback, is_child_algorithm=True)
        zone_inter = get_layer_obj(zone_inter_dict['OUTPUT'])

        # 4. Подсчет населения
        feedback.setProgressText("Этап 5: Подсчет населения...")
        pop_source = self.parameterAsSource(parameters, self.INPUT_BUILDINGS, context)
        pop_field = self.parameterAsString(parameters, self.POPULATION_FIELD, context)

        def calc_pop(zone_lyr, name):
            if not zone_lyr: return None
            total_pop = 0
            
            zone_iter = zone_lyr.getFeatures()
            zone_feat = next(zone_iter, None)
            
            if zone_feat:
                zone_geom = zone_feat.geometry()
                
                bld_idx = QgsSpatialIndex(pop_source.getFeatures(), feedback)
                candidate_ids = bld_idx.intersects(zone_geom.boundingBox())
                req = QgsFeatureRequest().setFilterFids(candidate_ids)
                
                for bld in pop_source.getFeatures(req):
                    if bld.geometry().intersects(zone_geom):
                        val = bld[pop_field]
                        if val: total_pop += val
            
            feedback.pushInfo(f"--- {name}: {int(total_pop)} чел. ---")
            
            # Добавление атрибута
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

        final_in = calc_pop(zone_in, "В РАЙОН")
        final_out = calc_pop(zone_out, "ИЗ РАЙОНА")
        final_inter = calc_pop(zone_inter, "ПЕРЕСЕЧЕНИЕ")

        # Сохранение результатов
        results = {}
        outputs_map = [
            (final_in, self.OUTPUT_LAYER_IN, "Зона_Вход"),
            (final_out, self.OUTPUT_LAYER_OUT, "Зона_Выход"),
            (final_inter, self.OUTPUT_INTERSECTION, "Зона_Общая")
        ]

        for layer, sink_name, debug_name in outputs_map:
            if layer:
                (sink, dest_id) = self.parameterAsSink(parameters, sink_name, context,
                                                       layer.fields(), layer.wkbType(), layer.sourceCrs())
                if sink:
                    sink.addFeatures(layer.getFeatures(), QgsFeatureSink.FastInsert)
                    results[sink_name] = dest_id
                    
                    # Для отладки добавляем на карту (можно закомментировать)
                    stored_lyr = QgsProcessingUtils.mapLayerFromString(dest_id, context)
                    if stored_lyr:
                        stored_lyr.setName(debug_name)
                        # QgsProject.instance().addMapLayer(stored_lyr)

        return results