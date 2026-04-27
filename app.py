import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Vmingo ERP - Robot Almacén", page_icon="🤖", layout="wide")

# =====================================================================
# FUNCIONES DE AYUDA BLINDADAS
# =====================================================================
def limpiar_nombre(texto):
    idx = texto.lower().find('detalle')
    if idx != -1: return texto[:idx].strip()
    return texto.strip()

def obtener_columna_exacta(cols_map, palabras_clave):
    # 1. Busca coincidencia exacta primero (para evitar que "sku quantity" le gane a "quantity")
    for palabra in palabras_clave:
        if palabra in cols_map: return cols_map[palabra]
    # 2. Si no hay exacta, busca parcial
    for palabra in palabras_clave:
        for key, val in cols_map.items():
            if palabra in key: return val
    return None

def detectar_plataforma_web(archivo_buffer):
    if archivo_buffer.name.endswith('.xlsx'):
        for i in range(10):
            try:
                df_temp = pd.read_excel(archivo_buffer, skiprows=i, nrows=2)
                cols = [str(c).lower().replace('ú','u').replace('ó','o').replace('í','i').strip() for c in df_temp.columns]
                if any('id del pedido' in c for c in cols) and any('sku de contribu' in c for c in cols): return 'TEMU', i
                if any('order id' in c or 'id de pedido' in c for c in cols) and any('seller sku' in c or 'sku del vendedor' in c for c in cols): return 'TIKTOK', i
                if any('numero de pedido' in c for c in cols) and any('sku' in c for c in cols) and not any('seller' in c or 'vendedor' in c for c in cols): return 'SHEIN', i
            except: pass
        return 'DESCONOCIDA', None
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
                    if ('order id' in lin_low or 'id de pedido' in lin_low) and ('seller sku' in lin_low or 'sku del vendedor' in lin_low): return 'TIKTOK', cod
                    if 'numero de pedido' in lin_low and 'sku' in lin_low and 'seller sku' not in lin_low: return 'SHEIN', cod
            except: pass
    return 'DESCONOCIDA', None

st.title("🤖 Vmingo ERP: Centro de Surtido y Empaque")

tab_picking, tab_robot = st.tabs(["🛒 FASE 1: Master Picking (Surtido)", "📦 FASE 2: Emparejador y Tickets (División Sagrada)"])

# =====================================================================
# PESTAÑA 1: FASE DE ALMACÉN (SABER QUÉ TRAER DE LOS PASILLOS)
# =====================================================================
with tab_picking:
    st.markdown("### 1. Extracción de Listas de Recolección")
    st.info("Sube los documentos para saber qué se necesita sacar para la Avalancha y los Carritos.")
    
    col_t, col_s, col_k = st.columns(3)
    with col_t: file_temu = st.file_uploader("A. Sube TEMU", type=["csv", "xlsx"], key="t_temu")
    with col_s: file_shein = st.file_uploader("B. Sube SHEIN", type=["csv", "xlsx"], key="t_shein")
    with col_k: file_tiktok = st.file_uploader("C. Sube TIKTOK (Archivo 1)", type=["csv", "xlsx"], key="t_tiktok")
        
    col_base, col_emp = st.columns([1, 2])
    with col_base:
        base_picking = st.file_uploader("D. BASE (Opcional)", type=["xlsx", "xlsm"], key="base_pick")
    with col_emp:
        empleados_input = st.text_input("Nombres del equipo para la Fase 2 (separados por coma):", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR")

    if st.button("📊 Generar Listas de Picking", type="primary"):
        archivos_subidos = [f for f in [file_temu, file_shein, file_tiktok] if f is not None]
        empleados = [e.strip().upper() for e in empleados_input.split(',') if e.strip()]
        
        if not archivos_subidos: st.error("❌ Sube al menos un archivo.")
        elif not empleados: st.error("❌ Necesitas ingresar al menos un nombre.")
        else:
            with st.spinner("Procesando pedidos..."):
                diccionario_nombres = {}
                if base_picking:
                    try:
                        df_base_mp = pd.read_excel(base_picking, sheet_name='BASE')
                        df_base_mp.columns = df_base_mp.columns.str.strip().str.upper() 
                        if 'SKU' in df_base_mp.columns and 'NOMBRE PLATAFORMA' in df_base_mp.columns:
                            for idx, fila in df_base_mp.iterrows():
                                sku = str(fila['SKU']).strip()
                                nombre = str(fila['NOMBRE PLATAFORMA']).strip()
                                if pd.notna(sku) and sku != 'nan': diccionario_nombres[sku] = nombre
                    except: pass

                dataframes_limpios = []
                for archivo in archivos_subidos:
                    plat, conf = detectar_plataforma_web(archivo)
                    archivo.seek(0)
                    
                    if plat == 'DESCONOCIDA': 
                        st.error(f"❌ ERROR: No pude reconocer el archivo '{archivo.name}'.")
                        continue
                        
                    if archivo.name.endswith('.xlsx'):
                        skip = conf if isinstance(conf, int) else 0
                        df_temp = pd.read_excel(archivo, skiprows=skip)
                    else:
                        texto_csv = archivo.getvalue().decode(conf)
                        skip_lineas = 0
                        for i, linea in enumerate(texto_csv.splitlines()[:15]):
                            lin_low = linea.lower().replace('ú', 'u').replace('ó', 'o').replace('í', 'i')
                            if (plat == 'TEMU' and 'id del pedido' in lin_low) or (plat == 'TIKTOK' and ('order id' in lin_low or 'id de pedido' in lin_low)) or (plat == 'SHEIN' and 'numero de pedido' in lin_low):
                                skip_lineas = i; break
                        archivo.seek(0) 
                        df_temp = pd.read_csv(archivo, skiprows=skip_lineas, encoding=conf)

                    cols_map = {c.lower().replace('ú', 'u').replace('ó', 'o').strip(): c for c in df_temp.columns}
                    
                    if plat == 'TEMU':
                        col_order = obtener_columna_exacta(cols_map, ['id del pedido'])
                        col_sku = obtener_columna_exacta(cols_map, ['sku de contribucion', 'sku'])
                        col_qty = obtener_columna_exacta(cols_map, ['cantidad a enviar', 'cantidad'])
                        col_var = obtener_columna_exacta(cols_map, ['variacion'])
                        col_nom = obtener_columna_exacta(cols_map, ['nombre del producto'])
                        df_limpio = df_temp[[c for c in [col_order, col_sku, col_qty, col_var, col_nom] if c]].copy()
                        df_limpio.rename(columns={col_order: 'PEDIDO', col_sku: 'SKU', col_qty: 'CANTIDAD', col_var: 'VARIACION', col_nom: 'NOMBRE_ORIGINAL'}, inplace=True)
                        if 'CANTIDAD' not in df_limpio.columns: df_limpio['CANTIDAD'] = 1
                        
                    elif plat == 'TIKTOK':
                        col_order = obtener_columna_exacta(cols_map, ['order id', 'id de pedido'])
                        col_sku = obtener_columna_exacta(cols_map, ['seller sku', 'sku del vendedor'])
                        col_qty = obtener_columna_exacta(cols_map, ['quantity', 'cantidad']) # Ahora es exacto
                        col_var = obtener_columna_exacta(cols_map, ['variation', 'variacion'])
                        col_nom = obtener_columna_exacta(cols_map, ['product name', 'nombre del producto'])
                        
                        df_limpio = df_temp[[c for c in [col_order, col_sku, col_qty, col_var, col_nom] if c]].copy()
                        df_limpio.rename(columns={col_order: 'PEDIDO', col_sku: 'SKU', col_qty: 'CANTIDAD', col_var: 'VARIACION', col_nom: 'NOMBRE_ORIGINAL'}, inplace=True)
                        if 'CANTIDAD' not in df_limpio.columns: df_limpio['CANTIDAD'] = 1
                        # FILTRO ANTIBASURA DE TIKTOK
                        df_limpio = df_limpio[~df_limpio['PEDIDO'].astype(str).str.contains('Platform|plataforma|unique|nan', case=False, na=False)]

                    elif plat == 'SHEIN':
                        col_order = obtener_columna_exacta(cols_map, ['numero de pedido'])
                        col_sku = obtener_columna_exacta(cols_map, ['sku del vendedor', 'sku'])
                        col_var = obtener_columna_exacta(cols_map, ['especificacion'])
                        col_nom = obtener_columna_exacta(cols_map, ['nombre del producto'])
                        df_limpio = df_temp[[c for c in [col_order, col_sku, col_var, col_nom] if c]].copy()
                        df_limpio.rename(columns={col_order: 'PEDIDO', col_sku: 'SKU', col_var: 'VARIACION', col_nom: 'NOMBRE_ORIGINAL'}, inplace=True)
                        df_limpio['CANTIDAD'] = 1
                        
                    df_limpio['PLATAFORMA'] = plat
                    df_limpio['ORDEN_ORIGINAL'] = range(len(df_limpio)) 
                    dataframes_limpios.append(df_limpio)

                if not dataframes_limpios: st.stop()

                # UNIFICACIÓN
                df_total = pd.concat(dataframes_limpios, ignore_index=True)
                df_total = df_total.dropna(subset=['PEDIDO'])
                df_total['PEDIDO'] = df_total['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '')).str.strip()
                df_total['SKU'] = df_total['SKU'].astype(str).str.strip()
                df_total['CANTIDAD'] = pd.to_numeric(df_total['CANTIDAD'], errors='coerce').fillna(1)
                
                df_total['Nombre Correcto'] = df_total.apply(
                    lambda fila: limpiar_nombre(diccionario_nombres.get(str(fila.get('SKU', '')).strip(), f"{fila.get('NOMBRE_ORIGINAL', '')} - Var: {fila.get('VARIACION', 'N/A')}")), axis=1
                )
                df_total['Nombre Correcto'] = df_total['Nombre Correcto'].fillna('SIN NOMBRE').astype(str)
                df_total['PEDIDO_DISPLAY'] = df_total['PEDIDO']
                
                # DETECTAR AVALANCHA VS CARRITO
                conteo_por_pedido = df_total.groupby('PEDIDO')['SKU'].nunique().reset_index()
                conteo_por_pedido.columns = ['PEDIDO', 'TIPOS_PRODUCTO']
                df_total = df_total.merge(conteo_por_pedido, on='PEDIDO')
                df_total['TIPO_SURTIDO'] = df_total['TIPOS_PRODUCTO'].apply(lambda x: 'AVALANCHA' if x == 1 else 'CARRITO')
                df_total['TRACKING_ID'] = "" 
                
                # GUARDAMOS EN MEMORIA PARA LA FASE 2
                st.session_state['master_df'] = df_total
                st.session_state['empleados_activos'] = empleados

                # CREAR EXCEL DE FASE 1 (SOLO LISTAS DE RECOLECCIÓN)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    formato_temu = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                    formato_shein = writer.book.add_format({'bg_color': '#D9EAD3', 'font_color': '#38761D'})
                    formato_tiktok = writer.book.add_format({'bg_color': '#CFE2F3', 'font_color': '#0B5394'})
                    
                    df_ava = df_total[df_total['TIPO_SURTIDO'] == 'AVALANCHA'].groupby(['PLATAFORMA', 'SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
                    df_ava = df_ava.sort_values(by='CANTIDAD', ascending=False)
                    df_ava.to_excel(writer, sheet_name='⚡ AVALANCHA', index=False)
                    ws_ava = writer.sheets['⚡ AVALANCHA']
                    ws_ava.set_column('A:A', 15); ws_ava.set_column('B:B', 20); ws_ava.set_column('C:C', 50); ws_ava.set_column('D:D', 12)
                    ws_ava.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'TEMU', 'format': formato_temu})
                    ws_ava.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'SHEIN', 'format': formato_shein})
                    ws_ava.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'TIKTOK', 'format': formato_tiktok})
                    
                    df_car = df_total[df_total['TIPO_SURTIDO'] == 'CARRITO'].groupby(['PLATAFORMA', 'SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
                    df_car = df_car.sort_values(by=['PLATAFORMA', 'Nombre Correcto'])
                    df_car.to_excel(writer, sheet_name='🛒 CARRITOS (Múltiples)', index=False)
                    ws_car = writer.sheets['🛒 CARRITOS (Múltiples)']
                    ws_car.set_column('A:A', 15); ws_car.set_column('B:B', 20); ws_car.set_column('C:C', 50); ws_car.set_column('D:D', 12)
                    ws_car.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'TEMU', 'format': formato_temu})
                    ws_car.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'SHEIN', 'format': formato_shein})
                    ws_car.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'TIKTOK', 'format': formato_tiktok})

                st.success("✅ ¡Robot memorizó todo! TikTok incluido. Entrega estas listas al almacén para surtir.")
                st.download_button("📥 Descargar Master Picking (Excel)", data=output.getvalue(), file_name=f"Picking_Almacen_{datetime.now().strftime('%d-%m-%Y')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")

# =====================================================================
# PESTAÑA 2: FASE DE PAQUETERÍA (LA DIVISIÓN COMO SIEMPRE SE HA HECHO)
# =====================================================================
with tab_robot:
    st.markdown("### 2. División de Guías y Generador de Tickets")
    
    if 'master_df' not in st.session_state:
        st.warning("⚠️ El robot no tiene pedidos en memoria. Primero realiza la Fase 1.")
    else:
        df_memoria = st.session_state['master_df']
        empleados_activos = st.session_state['empleados_activos']
        st.success(f"🧠 Memoria Activa: {df_memoria['PEDIDO'].nunique()} pedidos detectados. Listos para cruzar con PDFs y dividir entre {len(empleados_activos)} personas.")
        
        st.markdown("Sube los PDFs gigantes y el **Archivo 2 de TikTok** (el que trae los JMX). El robot emparejará todo y dividirá el empaque como siempre lo ha hecho.")

        col1, col2 = st.columns(2)
        with col1:
            pdf_temu = st.file_uploader("📦 PDF Guías TEMU", type=["pdf"])
            pdf_shein = st.file_uploader("📦 PDF Guías SHEIN", type=["pdf"])
        with col2:
            pdf_tiktok = st.file_uploader("📦 PDF Guías TIKTOK", type=["pdf"])
            tiktok_csv_2 = st.file_uploader("📝 Archivo 2 TIKTOK (Con JMX mezclados)", type=["csv", "xlsx"])

        if st.button("✂️ Cortar Guías y Crear Tickets Equitativos", type="primary"):
            if not (pdf_temu or pdf_shein or pdf_tiktok):
                st.error("❌ Sube al menos un archivo PDF de guías para cortar.")
                st.stop()
                
            with st.spinner("Emparejando y aplicando División Sagrada original..."):
                paginas_por_pedido = {} 
                
                # --- PROCESAR TIKTOK FASE 2 ---
                if pdf_tiktok and tiktok_csv_2:
                    plat, conf = detectar_plataforma_web(tiktok_csv_2)
                    tiktok_csv_2.seek(0)
                    if tiktok_csv_2.name.endswith('.xlsx'):
                        df_tk2 = pd.read_excel(tiktok_csv_2, skiprows=conf if isinstance(conf, int) else 0)
                    else:
                        texto_csv = tiktok_csv_2.getvalue().decode(conf)
                        skip = 0
                        for i, l in enumerate(texto_csv.splitlines()[:15]):
                            lin_low = l.lower().replace('ú','u').replace('ó','o').replace('í','i')
                            if 'order id' in lin_low or 'id de pedido' in lin_low: skip = i; break
                        tiktok_csv_2.seek(0)
                        df_tk2 = pd.read_csv(tiktok_csv_2, skiprows=skip, encoding=conf)
                    
                    cols_tk = {c.lower().replace('ú','u').replace('ó','o').strip(): c for c in df_tk2.columns}
                    col_order = obtener_columna_exacta(cols_tk, ['order id', 'id de pedido'])
                    col_track = obtener_columna_exacta(cols_tk, ['tracking id', 'id de seguimiento', 'numero de guia', 'guia'])
                    
                    if col_order and col_track:
                        df_tk2 = df_tk2[~df_tk2[col_order].astype(str).str.contains('Platform|plataforma|unique', case=False, na=False)]
                        df_tk2[col_order] = df_tk2[col_order].astype(str).apply(lambda x: x.replace('.0', '')).str.strip()
                        df_tk2[col_track] = df_tk2[col_track].astype(str).apply(lambda x: x.replace('.0', '')).str.strip()
                        
                        mapeo_jmx = dict(zip(df_tk2[col_order], df_tk2[col_track]))
                        df_memoria['TRACKING_ID'] = df_memoria.apply(
                            lambda row: mapeo_jmx.get(row['PEDIDO'], "") if row['PLATAFORMA'] == 'TIKTOK' else row['TRACKING_ID'], axis=1
                        )
                        
                    reader_tk = PyPDF2.PdfReader(pdf_tiktok)
                    jmx_actual = None
                    temp_jmx_pages = {}
                    for num, pag in enumerate(reader_tk.pages):
                        matches = re.findall(r'(JMX\d+)', pag.extract_text() or "")
                        if matches:
                            jmx_actual = str(matches[0]).strip()
                            if jmx_actual not in temp_jmx_pages: temp_jmx_pages[jmx_actual] = []
                            temp_jmx_pages[jmx_actual].append(pag)
                        else:
                            if jmx_actual: temp_jmx_pages[jmx_actual].append(pag)
                    
                    df_tk_memoria = df_memoria[df_memoria['PLATAFORMA'] == 'TIKTOK']
                    for idx, row in df_tk_memoria.iterrows():
                        jmx = row['TRACKING_ID']
                        pedido = row['PEDIDO']
                        if jmx in temp_jmx_pages: paginas_por_pedido[pedido] = temp_jmx_pages[jmx]
                            
                # --- PROCESAR TEMU ---
                if pdf_temu:
                    reader_temu = PyPDF2.PdfReader(pdf_temu)
                    po_actual = None 
                    for num, pag in enumerate(reader_temu.pages):
                        matches = re.findall(r'(PO-\d{3}-\d+)', pag.extract_text() or "")
                        if matches:
                            po_actual = str(matches[0]).strip()
                            if po_actual not in paginas_por_pedido:
                                paginas_por_pedido[po_actual] = []
                                if num > 0 and reader_temu.pages[num - 1] not in paginas_por_pedido[po_actual]:
                                    paginas_por_pedido[po_actual].append(reader_temu.pages[num - 1])
                            if pag not in paginas_por_pedido[po_actual]: paginas_por_pedido[po_actual].append(pag)

                # --- PROCESAR SHEIN ---
                if pdf_shein:
                    reader_shein = PyPDF2.PdfReader(pdf_shein)
                    chunks_shein = []
                    chunk_actual = []
                    for pag in reader_shein.pages:
                        texto = pag.extract_text() or ""
                        if re.search(r'(JMX|GSH|J&T|TODOOR|D2D)', texto.upper()) and 'DECLARACIÓN DE CONTENIDO' not in texto.upper():
                            if chunk_actual: chunks_shein.append(chunk_actual)
                            chunk_actual = [pag]
                        else:
                            if chunk_actual: chunk_actual.append(pag)
                            else: chunk_actual = [pag]
                    if chunk_actual: chunks_shein.append(chunk_actual)
                    
                    pedidos_shein_ordenados = df_memoria[df_memoria['PLATAFORMA'] == 'SHEIN'].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique()
                    for i, ped_shein in enumerate(pedidos_shein_ordenados):
                        if i < len(chunks_shein): paginas_por_pedido[ped_shein] = chunks_shein[i]

                # =================================================================
                # LA DIVISIÓN "COMO SIEMPRE SE HA HECHO"
                # =================================================================
                lista_pos_pdf = list(paginas_por_pedido.keys())
                df_ordenado = df_memoria[df_memoria['PEDIDO'].isin(lista_pos_pdf)].copy()
                
                if df_ordenado.empty:
                    st.error("❌ ERROR: Ningún pedido físico en el PDF coincidió con la memoria.")
                    st.stop()
                
                # Respetamos el orden de los PDFs tal cual se emparejaron
                df_ordenado['PEDIDO'] = pd.Categorical(df_ordenado['PEDIDO'], categories=lista_pos_pdf, ordered=True)
                df_ordenado = df_ordenado.sort_values('PEDIDO')

                # REPARTICIÓN MATEMÁTICA TRADICIONAL
                num_empleados = len(empleados_activos)
                pos_base = len(lista_pos_pdf) // num_empleados
                sobrantes = len(lista_pos_pdf) % num_empleados
                cantidades_por_empleado = [pos_base + (1 if i < sobrantes else 0) for i in range(num_empleados)]

                zip_buffer = io.BytesIO()
                colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
                
                with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        
                        indice_inicio = 0
                        for i, emp in enumerate(empleados_activos):
                            indice_fin = indice_inicio + cantidades_por_empleado[i]
                            pos_del_empleado = lista_pos_pdf[indice_inicio:indice_fin]
                            indice_inicio = indice_fin
                            
                            df_emp = df_ordenado[df_ordenado['PEDIDO'].isin(pos_del_empleado)].copy()
                            
                            if not df_emp.empty:
                                color_actual = colores_division[i % len(colores_division)]
                                
                                # -- EXCEL Y TICKETS --
                                worksheet = writer.book.add_worksheet(emp)
                                formato_titulo = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': color_actual, 'border': 1})
                                worksheet.write(0, 0, f"LISTA DE EMPAQUE PARA: {emp.upper()}", formato_titulo)
                                worksheet.write(1, 0, f"Total de guías asignadas: {len(pos_del_empleado)}")
                                
                                picking_list = df_emp.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                                picking_list.rename(columns={'Nombre Correcto': 'Descripción', 'CANTIDAD': 'Total a Empacar'}, inplace=True)
                                picking_list = picking_list.sort_values(by='Descripción').reset_index(drop=True)

                                inicio_t1 = 3
                                fin_t1 = inicio_t1 + len(picking_list)
                                picking_list.to_excel(writer, sheet_name=emp, index=False, header=False, startrow=inicio_t1 + 1, startcol=0)
                                worksheet.add_table(inicio_t1, 0, fin_t1, len(picking_list.columns) - 1, {
                                    'columns': [{'header': col} for col in picking_list.columns], 'style': 'Table Style Medium 9'
                                })
                                worksheet.set_column('A:A', 20); worksheet.set_column('B:B', 65)

                                fila_orden = fin_t1 + 3
                                worksheet.write(fila_orden, 0, f"ORDEN EXACTO DE GUÍAS DE {emp.upper()}:", formato_titulo)
                                df_orden_imp = df_emp.groupby(['PEDIDO_DISPLAY', 'SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                                df_orden_imp.rename(columns={'PEDIDO_DISPLAY': 'PEDIDO', 'CANTIDAD': 'Cant.'}, inplace=True)
                                df_orden_imp.to_excel(writer, sheet_name=emp, index=False, startrow=fila_orden + 2, startcol=0)
                                
                                # HOJA TICKET
                                hoja_ticket = writer.book.add_worksheet(f"{emp}_Ticket")
                                fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                                fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                                fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                                fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                                
                                num_division = i + 1
                                hoja_ticket.write('A1', f'DIVISION {num_division}', fmt_header)
                                hoja_ticket.write('D1', 'MIXTO', fmt_header) 
                                hoja_ticket.merge_range('A2:D2', emp.upper(), fmt_titulo_ticket)
                                
                                encabezados = ['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']
                                for col, encabezado in enumerate(encabezados):
                                    if encabezado == 'CANTI\nDAD': hoja_ticket.write(3, col, encabezado, fmt_wrap)
                                    else: hoja_ticket.write(3, col, encabezado, fmt_header)
                                    
                                total_piezas = 0
                                fila = 4
                                for idx, item in picking_list.iterrows():
                                    cant = int(item['Total a Empacar'])
                                    total_piezas += cant
                                    hoja_ticket.write(fila, 0, idx + 1, fmt_td_centro) 
                                    hoja_ticket.write(fila, 1, item['SKU'], fmt_td_centro)            
                                    hoja_ticket.write(fila, 2, item['Descripción'], fmt_td_izq)  
                                    hoja_ticket.write(fila, 3, cant, fmt_td_centro)     
                                    fila += 1
                                    
                                hoja_ticket.write(fila, 0, len(picking_list) + 1, fmt_td_centro)
                                hoja_ticket.merge_range(fila, 1, fila, 2, 'Total general', fmt_total)
                                hoja_ticket.write(fila, 3, total_piezas, fmt_total)
                                
                                hoja_ticket.set_column('A:A', 4); hoja_ticket.set_column('B:B', 16); hoja_ticket.set_column('C:C', 38); hoja_ticket.set_column('D:D', 6)
                                hoja_ticket.set_row(3, 30); hoja_ticket.set_row(1, 25) 
                                hoja_ticket.fit_to_pages(1, 0); hoja_ticket.set_margins(left=0.1, right=0.1, top=0.1, bottom=0.1) 

                                # -- EL PDF ORIGINAL -- (Un PDF por persona, sin separaciones raras)
                                pdf_emp_writer = PyPDF2.PdfWriter()
                                for po in pos_del_empleado:
                                    if po in paginas_por_pedido:
                                        for p in paginas_por_pedido[po]: pdf_emp_writer.add_page(p)
                                pdf_emp_buf = io.BytesIO()
                                pdf_emp_writer.write(pdf_emp_buf)
                                zip_file.writestr(f"Guias_{emp}.pdf", pdf_emp_buf.getvalue())

                    zip_file.writestr("Tickets_y_Reparticion.xlsx", excel_buffer.getvalue())
                st.session_state['descarga_pdfs'] = zip_buffer.getvalue()

        if 'descarga_pdfs' in st.session_state:
            st.balloons()
            st.success("✂️ ¡División como siempre se ha hecho completada! Revisa tu ZIP.")
            st.download_button(
                label="📦 Descargar ZIP (Tickets y Guías)",
                data=st.session_state['descarga_pdfs'],
                file_name=f"Guias_Empaque_Vmingo_{datetime.now().strftime('%d-%m-%Y')}.zip",
                mime="application/zip",
                type="primary"
            )
