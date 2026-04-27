import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Robot Multiplataforma Vmingo", page_icon="🤖", layout="wide")

def limpiar_nombre(texto):
    idx = texto.lower().find('detalle')
    if idx != -1:
        return texto[:idx].strip()
    return texto.strip()

def detectar_plataforma_web(archivo_buffer):
    if archivo_buffer.name.endswith('.xlsx'):
        df_temp = pd.read_excel(archivo_buffer, nrows=5)
        cols = [str(c).lower() for c in df_temp.columns]
        if 'id del pedido' in cols and 'sku de contribución' in cols: return 'TEMU', None
        if 'order id' in cols and 'seller sku' in cols: return 'TIKTOK', None
        if 'número de pedido' in cols and 'sku del vendedor' in cols: return 'SHEIN', None
        for i in range(1, 10):
            try:
                df_temp = pd.read_excel(archivo_buffer, skiprows=i, nrows=2)
                cols = [str(c).lower() for c in df_temp.columns]
                if 'id del pedido' in cols and 'sku de contribución' in cols: return 'TEMU', i
                if 'order id' in cols and 'seller sku' in cols: return 'TIKTOK', i
                if 'número de pedido' in cols and 'sku del vendedor' in cols: return 'SHEIN', i
            except: pass
    else:
        encodings_a_probar = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252']
        contenido = archivo_buffer.getvalue()
        for cod in encodings_a_probar:
            try:
                texto = contenido.decode(cod)
                lineas = texto.splitlines()
                for i, linea in enumerate(lineas[:15]):
                    lin_low = linea.lower().replace('ú', 'u').replace('ó', 'o').replace('í', 'i')
                    if 'id del pedido' in lin_low and 'sku de contribu' in lin_low: return 'TEMU', cod
                    if 'order id' in lin_low and 'seller sku' in lin_low: return 'TIKTOK', cod
                    if 'numero de pedido' in lin_low and 'sku' in lin_low: return 'SHEIN', cod
            except: pass
    return 'DESCONOCIDA', None

st.title("🤖 Robot Multiplataforma Vmingo")

tab_picking, tab_robot = st.tabs(["🛒 1. Master Picking (Surtido de Almacén)", "📦 2. Robot Repartidor (PDFs y Empaque)"])

# =====================================================================
# PESTAÑA 1: EL GENERADOR DE MASTER PICKING
# =====================================================================
with tab_picking:
    st.markdown("### Generador de Picking: Avalancha y Carritos")
    st.info("Sube tus documentos exportados (Excel o CSV). El sistema juntará todo y separará lo masivo de lo que requiere carrito.")
    
    col_t, col_s, col_k = st.columns(3)
    with col_t: file_temu = st.file_uploader("A. Sube TEMU", type=["csv", "xlsx"], key="t_temu")
    with col_s: file_shein = st.file_uploader("B. Sube SHEIN", type=["csv", "xlsx"], key="t_shein")
    with col_k: file_tiktok = st.file_uploader("C. Sube TIKTOK", type=["csv", "xlsx"], key="t_tiktok")
        
    base_picking = st.file_uploader("D. Sube tu BASE de datos (Opcional)", type=["xlsx", "xlsm"], key="base_pick")

    if st.button("📊 Generar Master Picking", type="primary"):
        archivos_subidos = [f for f in [file_temu, file_shein, file_tiktok] if f is not None]
        if not archivos_subidos:
            st.error("Sube al menos un archivo para generar el picking.")
        else:
            with st.spinner("Consolidando plataformas..."):
                diccionario_nombres_mp = {}
                if base_picking:
                    try:
                        df_base_mp = pd.read_excel(base_picking, sheet_name='BASE')
                        df_base_mp.columns = df_base_mp.columns.str.strip().str.upper() 
                        if 'SKU' in df_base_mp.columns and 'NOMBRE PLATAFORMA' in df_base_mp.columns:
                            for idx, fila in df_base_mp.iterrows():
                                sku = str(fila['SKU']).strip()
                                nombre = str(fila['NOMBRE PLATAFORMA']).strip()
                                if pd.notna(sku) and sku != 'nan':
                                    diccionario_nombres_mp[sku] = nombre
                    except: pass

                dataframes_limpios = []
                for archivo in archivos_subidos:
                    plat, conf = detectar_plataforma_web(archivo)
                    archivo.seek(0)
                    if plat != 'DESCONOCIDA':
                        if archivo.name.endswith('.xlsx'):
                            skip = conf if isinstance(conf, int) else 0
                            df_temp = pd.read_excel(archivo, skiprows=skip)
                        else:
                            texto_csv = archivo.getvalue().decode(conf)
                            lineas = texto_csv.splitlines()
                            skip_lineas = 0
                            for i, linea in enumerate(lineas[:15]):
                                lin_low = linea.lower().replace('ú', 'u').replace('ó', 'o').replace('í', 'i')
                                if (plat == 'TEMU' and 'id del pedido' in lin_low) or (plat == 'TIKTOK' and 'order id' in lin_low) or (plat == 'SHEIN' and 'numero de pedido' in lin_low):
                                    skip_lineas = i
                                    break
                            archivo.seek(0) 
                            df_temp = pd.read_csv(archivo, skiprows=skip_lineas, encoding=conf)

                        cols_map = {c.lower().replace('ú', 'u').replace('ó', 'o').strip(): c for c in df_temp.columns}
                        
                        if plat == 'TEMU':
                            df_limpio = df_temp[[cols_map.get('id del pedido'), cols_map.get('sku de contribucion'), cols_map.get('cantidad a enviar')]].copy()
                            df_limpio.columns = ['PEDIDO', 'SKU', 'CANTIDAD']
                        elif plat == 'TIKTOK':
                            col_order = cols_map.get('order id')
                            col_sku = cols_map.get('seller sku')
                            col_var = cols_map.get('variation')
                            df_limpio = df_temp[[col_order, col_sku, cols_map.get('quantity'), col_var]].copy()
                            if col_order and col_var: df_limpio = df_limpio.drop_duplicates(subset=[col_order, col_var])
                            df_limpio = df_limpio[['Order ID' if not col_order else col_order, 'Seller SKU' if not col_sku else col_sku, 'Quantity' if not cols_map.get('quantity') else cols_map.get('quantity')]].copy()
                            df_limpio.columns = ['PEDIDO', 'SKU', 'CANTIDAD']
                        elif plat == 'SHEIN':
                            df_limpio = df_temp[[cols_map.get('numero de pedido'), cols_map.get('sku del vendedor')]].copy()
                            df_limpio.columns = ['PEDIDO', 'SKU']
                            df_limpio['CANTIDAD'] = 1
                            
                        df_limpio['PLATAFORMA'] = plat
                        dataframes_limpios.append(df_limpio)

                df_total = pd.concat(dataframes_limpios, ignore_index=True)
                df_total = df_total.dropna(subset=['PEDIDO'])
                df_total['SKU'] = df_total['SKU'].astype(str).str.strip()
                df_total['CANTIDAD'] = pd.to_numeric(df_total['CANTIDAD'], errors='coerce').fillna(1)
                df_total['NOMBRE_PRODUCTO'] = df_total['SKU'].apply(lambda x: limpiar_nombre(diccionario_nombres_mp.get(x, "Sin registro en BASE")))
                
                conteo_por_pedido = df_total.groupby('PEDIDO')['SKU'].nunique().reset_index()
                conteo_por_pedido.columns = ['PEDIDO', 'TIPOS_PRODUCTO']
                df_total = df_total.merge(conteo_por_pedido, on='PEDIDO')
                
                df_avalancha = df_total[df_total['TIPOS_PRODUCTO'] == 1].groupby(['PLATAFORMA', 'SKU', 'NOMBRE_PRODUCTO'])['CANTIDAD'].sum().reset_index()
                df_avalancha = df_avalancha.sort_values(by='CANTIDAD', ascending=False)
                
                df_carrito = df_total[df_total['TIPOS_PRODUCTO'] > 1].groupby(['PLATAFORMA', 'SKU', 'NOMBRE_PRODUCTO'])['CANTIDAD'].sum().reset_index()
                df_carrito = df_carrito.sort_values(by='CANTIDAD', ascending=False)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_avalancha.to_excel(writer, sheet_name='Masivo (Avalancha)', index=False)
                    df_carrito.to_excel(writer, sheet_name='Carritos (Múltiples)', index=False)
                    for sheet in ['Masivo (Avalancha)', 'Carritos (Múltiples)']:
                        ws = writer.sheets[sheet]
                        ws.set_column('A:A', 15)
                        ws.set_column('B:B', 20)
                        ws.set_column('C:C', 50)
                        ws.set_column('D:D', 12)
                        formato_temu = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                        ws.conditional_format('A2:A1000', {'type': 'text', 'criteria': 'containing', 'value': 'TEMU', 'format': formato_temu})

                st.success("✅ ¡Master Picking Listo!")
                st.write("🔥 **Top 3 Productos para Surtido Masivo (Avalancha):**")
                st.dataframe(df_avalancha.head(3), use_container_width=True)
                st.download_button("📥 Descargar Master Picking (Excel)", data=output.getvalue(), file_name=f"Master_Picking_{datetime.now().strftime('%d-%m-%Y')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")


# =====================================================================
# PESTAÑA 2: EL ROBOT REPARTIDOR DE PDFs (CON DOBLE REPARTICIÓN)
# =====================================================================
with tab_robot:
    st.markdown("Sube tus documentos para repartir las guías y dividirlas en **Avalancha** y **Carrito** por empleado.")

    col1, col2 = st.columns(2)
    with col1:
        archivo_csv = st.file_uploader("1. Sube el Documento Exportado (CSV / Excel)", type=["csv", "xlsx"], key="rob_doc")
        archivo_pdf = st.file_uploader("2. Sube el PDF gigante", type=["pdf"], key="rob_pdf")
    with col2:
        archivo_base = st.file_uploader("3. Sube tu BASE (Opcional)", type=["xlsx", "xlsm"], key="rob_base")

    empleados_input = st.text_input("Nombres del equipo en turno (separados por coma):", "ALDO, BELEN, ALDRICH")

    if st.button("🚀 Procesar Guías y Empaque", type="primary"):
        if not archivo_csv or not archivo_pdf:
            st.error("❌ Error: Faltan archivos.")
            st.stop()

        empleados = [e.strip() for e in empleados_input.split(',') if e.strip()]
        if not empleados:
            st.error("❌ Error: Necesitas ingresar al menos un nombre.")
            st.stop()

        with st.spinner("Analizando documentos y dividiendo cargas..."):
            plataforma, codificacion = detectar_plataforma_web(archivo_csv)
            if plataforma == 'DESCONOCIDA':
                st.error("❌ ERROR: No pude identificar la plataforma.")
                st.stop()
                
            st.success(f"¡Plataforma detectada!: **{plataforma}**")

            # --- LEYENDO BASE ---
            diccionario_nombres = {}
            if archivo_base:
                try:
                    df_base = pd.read_excel(archivo_base, sheet_name='BASE')
                    df_base.columns = df_base.columns.str.strip().str.upper() 
                    if 'SKU' in df_base.columns and 'NOMBRE PLATAFORMA' in df_base.columns:
                        for idx, fila in df_base.iterrows():
                            sku = str(fila['SKU']).strip()
                            nombre = str(fila['NOMBRE PLATAFORMA']).strip()
                            if pd.notna(sku) and sku != 'nan':
                                diccionario_nombres[sku] = nombre
                except: pass

            # --- 1. LEYENDO CSV/EXCEL ---
            archivo_csv.seek(0)
            if archivo_csv.name.endswith('.xlsx'):
                skip = codificacion if isinstance(codificacion, int) else 0
                df = pd.read_excel(archivo_csv, skiprows=skip)
            else:
                texto_csv = archivo_csv.getvalue().decode(codificacion)
                lineas = texto_csv.splitlines()
                skip_lineas = 0
                for i, linea in enumerate(lineas[:15]):
                    lin_low = linea.lower().replace('ú', 'u').replace('ó', 'o').replace('í', 'i')
                    if (plataforma == 'TEMU' and 'id del pedido' in lin_low) or (plataforma == 'TIKTOK' and 'order id' in lin_low) or (plataforma == 'SHEIN' and 'numero de pedido' in lin_low):
                        skip_lineas = i
                        break
                archivo_csv.seek(0) 
                df = pd.read_csv(archivo_csv, skiprows=skip_lineas, encoding=codificacion)
            
            cols_map = {c.lower().replace('ú', 'u').replace('ó', 'o').strip(): c for c in df.columns}

            mapa_pedidos_tiktok = {}
            if plataforma == 'TIKTOK':
                col_order = cols_map.get('order id')
                col_track = cols_map.get('tracking id')
                for idx, row in df.iterrows():
                    order_id = str(row.get(col_order, '')).replace('.0', '').strip() if col_order else ''
                    tracking_id = str(row.get(col_track, '')).replace('.0', '').strip() if col_track else ''
                    if order_id and order_id != 'nan': mapa_pedidos_tiktok[order_id] = order_id
                    if tracking_id and tracking_id != 'nan': mapa_pedidos_tiktok[tracking_id] = order_id

            if plataforma == 'TEMU':
                df_filtrado = df[[cols_map.get('id del pedido'), cols_map.get('sku de contribucion'), cols_map.get('nombre del producto'), cols_map.get('variacion'), cols_map.get('cantidad a enviar')]].copy()
                df_filtrado.columns = ['PEDIDO', 'SKU', 'NOMBRE_ORIGINAL', 'VARIACION', 'CANTIDAD']
            elif plataforma == 'TIKTOK':
                col_order = cols_map.get('order id')
                col_var = cols_map.get('variation')
                columnas_utiles = [c for c in [col_order, cols_map.get('seller sku'), cols_map.get('product name'), col_var, cols_map.get('quantity')] if c]
                df_filtrado = df[columnas_utiles].copy()
                if col_order and col_var: df_filtrado = df_filtrado.drop_duplicates(subset=[col_order, col_var])
                if col_order: df_filtrado['PEDIDO'] = df_filtrado[col_order].astype(str).str.strip()
                df_filtrado.rename(columns={cols_map.get('seller sku'): 'SKU', cols_map.get('product name'): 'NOMBRE_ORIGINAL', col_var: 'VARIACION', cols_map.get('quantity'): 'CANTIDAD'}, inplace=True)
            elif plataforma == 'SHEIN':
                col_pedido = cols_map.get('numero de pedido')
                columnas_utiles = [c for c in [col_pedido, cols_map.get('sku del vendedor'), cols_map.get('nombre del producto'), cols_map.get('especificacion')] if c]
                df_filtrado = df[columnas_utiles].copy()
                df_filtrado['CANTIDAD'] = 1
                if col_pedido: df_filtrado['PEDIDO'] = df_filtrado[col_pedido].astype(str).str.strip()
                df_filtrado.rename(columns={cols_map.get('sku del vendedor'): 'SKU', cols_map.get('nombre del producto'): 'NOMBRE_ORIGINAL', cols_map.get('especificacion'): 'VARIACION'}, inplace=True)

            df_filtrado = df_filtrado.dropna(subset=['PEDIDO'])
            df_filtrado['PEDIDO'] = df_filtrado['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
            df_filtrado['PEDIDO_DISPLAY'] = df_filtrado['PEDIDO']
            df_filtrado['SKU'] = df_filtrado.get('SKU', pd.Series(dtype=str)).fillna('SIN SKU').astype(str)
            df_filtrado['CANTIDAD'] = pd.to_numeric(df_filtrado.get('CANTIDAD', pd.Series(dtype=int)), errors='coerce').fillna(0)
            df_filtrado = df_filtrado[df_filtrado['CANTIDAD'] > 0]

            df_filtrado['Nombre Correcto'] = df_filtrado.apply(lambda fila: limpiar_nombre(diccionario_nombres.get(str(fila.get('SKU', '')).strip(), f"{fila.get('NOMBRE_ORIGINAL', '')} - Var: {fila.get('VARIACION', 'N/A')}")), axis=1)
            df_filtrado['Nombre Correcto'] = df_filtrado['Nombre Correcto'].fillna('SIN NOMBRE').astype(str)

            pos_finales_reales = list(dict.fromkeys(df_filtrado['PEDIDO'].tolist()))

            # --- 2. LEYENDO PDF (3 CEREBROS INDEPENDIENTES) ---
            paginas_por_po = {}
            reader = PyPDF2.PdfReader(archivo_pdf)
            
            if plataforma == 'SHEIN':
                chunks_pdf = []
                chunk_actual = []
                for num_pagina, pagina in enumerate(reader.pages):
                    texto = pagina.extract_text() or ""
                    if re.search(r'(JMX|GSH|J&T|TODOOR|D2D)', texto.upper()) and 'DECLARACIÓN DE CONTENIDO' not in texto.upper():
                        if chunk_actual: chunks_pdf.append(chunk_actual)
                        chunk_actual = [pagina]
                    else:
                        if chunk_actual: chunk_actual.append(pagina)
                        else: chunk_actual = [pagina]
                if chunk_actual: chunks_pdf.append(chunk_actual)

                for i, pedido_gsh in enumerate(pos_finales_reales):
                    if i < len(chunks_pdf): paginas_por_po[pedido_gsh] = chunks_pdf[i]
                    else: paginas_por_po[pedido_gsh] = []

            elif plataforma == 'TEMU':
                po_actual = None 
                for num_pagina, pagina in enumerate(reader.pages):
                    matches = re.findall(r'(PO-\d{3}-\d+)', pagina.extract_text() or "")
                    if matches:
                        po_actual = str(matches[0]).strip()
                        if po_actual not in paginas_por_po:
                            paginas_por_po[po_actual] = []
                            if num_pagina > 0 and reader.pages[num_pagina - 1] not in paginas_por_po[po_actual]:
                                paginas_por_po[po_actual].append(reader.pages[num_pagina - 1])
                        if pagina not in paginas_por_po[po_actual]: paginas_por_po[po_actual].append(pagina)

            elif plataforma == 'TIKTOK':
                po_actual = None 
                for num_pagina, pagina in enumerate(reader.pages):
                    matches = re.findall(r'(JMX\d+)', pagina.extract_text() or "")
                    if matches:
                        po_actual = str(matches[0]).strip()
                        if po_actual not in paginas_por_po: paginas_por_po[po_actual] = []
                        if pagina not in paginas_por_po[po_actual]: paginas_por_po[po_actual].append(pagina)
                    else:
                        if po_actual and pagina not in paginas_por_po[po_actual]: paginas_por_po[po_actual].append(pagina)

                paginas_corregidas = {}
                for jmx_key, paginas in paginas_por_po.items():
                    order_id = mapa_pedidos_tiktok.get(jmx_key, jmx_key)
                    if order_id not in paginas_corregidas: paginas_corregidas[order_id] = []
                    for pag in paginas:
                        if pag not in paginas_corregidas[order_id]: paginas_corregidas[order_id].append(pag)
                paginas_por_po = paginas_corregidas
                
            lista_pos_pdf = list(paginas_por_po.keys())
            pos_finales_reales = [po for po in pos_finales_reales if po in lista_pos_pdf]

            df_ordenado = pd.concat([df_filtrado[df_filtrado['PEDIDO'] == po].copy() for po in pos_finales_reales if not df_filtrado[df_filtrado['PEDIDO'] == po].empty])

            if df_ordenado.empty:
                st.error("❌ ERROR: Ningún pedido físico en el PDF coincidió con tu Excel.")
                st.stop()

            # =================================================================
            # DIVISIÓN INTELIGENTE: AVALANCHA Y CARRITO
            # =================================================================
            st.success(f"📄 ÉXITO: {len(pos_finales_reales)} pedidos físicos encontrados. Dividiendo en Avalancha y Carrito...")

            conteo_skus = df_ordenado.groupby('PEDIDO')['SKU'].nunique()
            lista_po_avalancha = conteo_skus[conteo_skus == 1].index.tolist()
            lista_po_carrito = conteo_skus[conteo_skus > 1].index.tolist()

            # Respetamos el orden físico del PDF para no cruzar guías
            lista_po_avalancha = [po for po in pos_finales_reales if po in lista_po_avalancha]
            lista_po_carrito = [po for po in pos_finales_reales if po in lista_po_carrito]

            num_empleados = len(empleados)

            # Division Avalancha
            base_ava = len(lista_po_avalancha) // num_empleados
            sob_ava = len(lista_po_avalancha) % num_empleados
            cant_emp_ava = [base_ava + (1 if i < sob_ava else 0) for i in range(num_empleados)]

            # Division Carrito
            base_car = len(lista_po_carrito) // num_empleados
            sob_car = len(lista_po_carrito) % num_empleados
            cant_emp_car = [base_car + (1 if i < sob_car else 0) for i in range(num_empleados)]

            colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    
                    idx_inicio_ava = 0
                    idx_inicio_car = 0
                    
                    for i, emp in enumerate(empleados):
                        color_actual = colores_division[i % len(colores_division)]
                        
                        # Obtener rebanadas de POs
                        idx_fin_ava = idx_inicio_ava + cant_emp_ava[i]
                        pos_emp_ava = lista_po_avalancha[idx_inicio_ava:idx_fin_ava]
                        idx_inicio_ava = idx_fin_ava
                        
                        idx_fin_car = idx_inicio_car + cant_emp_car[i]
                        pos_emp_car = lista_po_carrito[idx_inicio_car:idx_fin_car]
                        idx_inicio_car = idx_fin_car
                        
                        df_emp_ava = df_ordenado[df_ordenado['PEDIDO'].isin(pos_emp_ava)].copy()
                        df_emp_car = df_ordenado[df_ordenado['PEDIDO'].isin(pos_emp_car)].copy()
                        
                        worksheet = writer.book.add_worksheet(emp)
                        formato_titulo_ava = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': '#FFE699', 'border': 1})
                        formato_titulo_car = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': '#9BC2E6', 'border': 1})
                        
                        worksheet.write(0, 0, f"HOJA DE TRABAJO: {emp.upper()}", writer.book.add_format({'bold': True, 'font_size': 16}))
                        
                        fila_actual = 2
                        
                        # --- SECCIÓN AVALANCHA EN EXCEL ---
                        if not df_emp_ava.empty:
                            worksheet.write(fila_actual, 0, f"⚡ TU AVALANCHA ({len(pos_emp_ava)} Guías)", formato_titulo_ava)
                            fila_actual += 2
                            
                            pick_ava = df_emp_ava.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                            pick_ava.rename(columns={'Nombre Correcto': 'Descripción', 'CANTIDAD': 'Total'}, inplace=True)
                            pick_ava = pick_ava.sort_values(by='Descripción').reset_index(drop=True)
                            
                            inicio_ava = fila_actual
                            fin_ava = inicio_ava + len(pick_ava)
                            pick_ava.to_excel(writer, sheet_name=emp, index=False, header=False, startrow=inicio_ava + 1, startcol=0)
                            worksheet.add_table(inicio_ava, 0, fin_ava, len(pick_ava.columns) - 1, {'columns': [{'header': c} for c in pick_ava.columns], 'style': 'Table Style Light 19'})
                            fila_actual = fin_ava + 3
                        
                        # --- SECCIÓN CARRITO EN EXCEL ---
                        if not df_emp_car.empty:
                            worksheet.write(fila_actual, 0, f"🛒 TU CARRITO ({len(pos_emp_car)} Guías)", formato_titulo_car)
                            fila_actual += 2
                            
                            pick_car = df_emp_car.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                            pick_car.rename(columns={'Nombre Correcto': 'Descripción', 'CANTIDAD': 'Total'}, inplace=True)
                            pick_car = pick_car.sort_values(by='Descripción').reset_index(drop=True)
                            
                            inicio_car = fila_actual
                            fin_car = inicio_car + len(pick_car)
                            pick_car.to_excel(writer, sheet_name=emp, index=False, header=False, startrow=inicio_car + 1, startcol=0)
                            worksheet.add_table(inicio_car, 0, fin_car, len(pick_car.columns) - 1, {'columns': [{'header': c} for c in pick_car.columns], 'style': 'Table Style Light 9'})
                        
                        worksheet.set_column('A:A', 20)
                        worksheet.set_column('B:B', 65)

                        # --- CREACIÓN DE PDFs FÍSICOS SEPARADOS ---
                        # Generamos PDF de Avalancha para este empleado
                        if pos_emp_ava:
                            pdf_ava_writer = PyPDF2.PdfWriter()
                            for po in pos_emp_ava:
                                if po in paginas_por_po:
                                    for pagina in paginas_por_po[po]:
                                        pdf_ava_writer.add_page(pagina)
                            pdf_ava_buffer = io.BytesIO()
                            pdf_ava_writer.write(pdf_ava_buffer)
                            zip_file.writestr(f"1_Avalancha_{emp}.pdf", pdf_ava_buffer.getvalue())

                        # Generamos PDF de Carrito para este empleado
                        if pos_emp_car:
                            pdf_car_writer = PyPDF2.PdfWriter()
                            for po in pos_emp_car:
                                if po in paginas_por_po:
                                    for pagina in paginas_por_po[po]:
                                        pdf_car_writer.add_page(pagina)
                            pdf_car_buffer = io.BytesIO()
                            pdf_car_writer.write(pdf_car_buffer)
                            zip_file.writestr(f"2_Carrito_{emp}.pdf", pdf_car_buffer.getvalue())

                zip_file.writestr(f"Reparticion_FINAL_{plataforma}.xlsx", excel_buffer.getvalue())

            st.session_state['descarga_lista'] = zip_buffer.getvalue()
            st.session_state['plataforma_procesada'] = plataforma

if 'descarga_lista' in st.session_state:
    st.balloons()
    plat = st.session_state.get('plataforma_procesada', 'Vmingo')
    fecha = datetime.now().strftime("%d-%m-%Y")
    nombre_archivo = f"Guias_{plat}_Divididas_{fecha}.zip"
    
    st.success(f"✨ ¡Todo listo! Se ha generado el archivo {nombre_archivo} con el Excel de repartición exacta y los PDFs individuales separados por etapa.")
    
    st.download_button(
        label=f"📦 Descargar {nombre_archivo}",
        data=st.session_state['descarga_lista'],
        file_name=nombre_archivo,
        mime="application/zip",
        type="primary"
    )
