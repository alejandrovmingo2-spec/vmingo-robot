import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Robot Multiplataforma Vmingo", page_icon="🤖", layout="wide")

# =====================================================================
# FUNCIONES ORIGINALES DE ALEJANDRO
# =====================================================================
def limpiar_nombre(texto):
    idx = texto.lower().find('detalle')
    if idx != -1:
        return texto[:idx].strip()
    return texto.strip()

def detectar_plataforma_web(archivo_csv_buffer):
    encodings_a_probar = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252']
    contenido = archivo_csv_buffer.getvalue()
    for cod in encodings_a_probar:
        try:
            texto = contenido.decode(cod)
            lineas = texto.splitlines()
            for linea in lineas[:5]:
                lin_low = linea.lower()
                if 'id del pedido' in lin_low and 'sku de contribución' in lin_low:
                    return 'TEMU', cod
                # Búsqueda flexible para TikTok por si las dudas
                if ('order id' in lin_low or 'id de pedido' in lin_low) and ('seller sku' in lin_low or 'sku del vendedor' in lin_low):
                    return 'TIKTOK', cod
                if 'número de pedido' in lin_low and 'sku del vendedor' in lin_low:
                    return 'SHEIN', cod
        except:
            pass
    return 'DESCONOCIDA', None

st.title("🤖 Vmingo ERP: Centro de Surtido y Empaque")

tab_picking, tab_robot = st.tabs(["🛒 FASE 1: Master Picking (Surtido)", "📦 FASE 2: Emparejador y Tickets (División Normal)"])

# =====================================================================
# PESTAÑA 1: FASE DE ALMACÉN
# =====================================================================
with tab_picking:
    st.markdown("### 1. Extracción de Listas de Recolección")
    st.info("Sube los documentos en formato CSV. El robot separará el TOP 5 Avalancha y asignará los pedidos.")
    
    col_t, col_s, col_k = st.columns(3)
    with col_t: file_temu = st.file_uploader("A. Sube TEMU (CSV)", type=["csv"], key="t_temu")
    with col_s: file_shein = st.file_uploader("B. Sube SHEIN (CSV)", type=["csv"], key="t_shein")
    with col_k: file_tiktok = st.file_uploader("C. Sube TIKTOK 1 (CSV)", type=["csv"], key="t_tiktok")
        
    col_base, col_emp = st.columns([1, 2])
    with col_base:
        base_picking = st.file_uploader("D. BASE (Opcional)", type=["xlsx", "xlsm"], key="base_pick")
    with col_emp:
        empleados_input = st.text_input("Nombres del equipo en turno (separados por coma):", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR")

    if st.button("📊 Generar Listas de Picking", type="primary"):
        archivos_subidos = [f for f in [file_temu, file_shein, file_tiktok] if f is not None]
        empleados = [e.strip().upper() for e in empleados_input.split(',') if e.strip()]
        
        if not archivos_subidos: st.error("❌ Sube al menos un archivo CSV.")
        elif not empleados: st.error("❌ Necesitas ingresar al menos un nombre en el equipo.")
        else:
            with st.spinner("Analizando CSVs con la lógica original..."):
                diccionario_nombres = {}
                if base_picking:
                    try:
                        df_base = pd.read_excel(base_picking, sheet_name='BASE')
                        df_base.columns = df_base.columns.str.strip().str.upper() 
                        if 'SKU' in df_base.columns and 'NOMBRE PLATAFORMA' in df_base.columns:
                            for idx, fila in df_base.iterrows():
                                sku = str(fila['SKU']).strip()
                                nombre = str(fila['NOMBRE PLATAFORMA']).strip()
                                if pd.notna(sku) and sku != 'nan': diccionario_nombres[sku] = nombre
                    except Exception as e: st.warning(f"⚠️ Detalle en BASE: {e}")

                dataframes_limpios = []
                for archivo_csv in archivos_subidos:
                    plataforma, codificacion = detectar_plataforma_web(archivo_csv)
                    if plataforma == 'DESCONOCIDA':
                        st.error(f"❌ ERROR: No reconocí el archivo {archivo_csv.name}. Revisa las columnas.")
                        continue
                        
                    # LÓGICA DE SALTO DE LÍNEAS ORIGINAL
                    archivo_csv.seek(0)
                    texto_csv = archivo_csv.getvalue().decode(codificacion)
                    lineas = texto_csv.splitlines()
                    skip_lineas = 0
                    for i, linea in enumerate(lineas):
                        lin_low = linea.lower()
                        if (plataforma == 'TEMU' and 'id del pedido' in lin_low) or \
                           (plataforma == 'TIKTOK' and ('order id' in lin_low or 'id de pedido' in lin_low)) or \
                           (plataforma == 'SHEIN' and 'número de pedido' in lin_low):
                            skip_lineas = i
                            break
                            
                    archivo_csv.seek(0) 
                    df = pd.read_csv(archivo_csv, skiprows=skip_lineas, encoding=codificacion)
                    cols_map = {c.lower().strip(): c for c in df.columns}

                    # FILTRADO ORIGINAL
                    if plataforma == 'TEMU':
                        col_pedido = cols_map.get('id del pedido')
                        col_sku = cols_map.get('sku de contribución', cols_map.get('sku de contribucion'))
                        col_nombre = cols_map.get('nombre del producto')
                        col_var = cols_map.get('variación', cols_map.get('variacion'))
                        col_cant = cols_map.get('cantidad a enviar')
                        columnas_utiles = [c for c in [col_pedido, col_sku, col_nombre, col_var, col_cant] if c]
                        df_filtrado = df[columnas_utiles].copy()
                        rename_dict = {}
                        if col_pedido: rename_dict[col_pedido] = 'PEDIDO'
                        if col_sku: rename_dict[col_sku] = 'SKU'
                        if col_nombre: rename_dict[col_nombre] = 'NOMBRE_ORIGINAL'
                        if col_var: rename_dict[col_var] = 'VARIACION'
                        if col_cant: rename_dict[col_cant] = 'CANTIDAD'
                        df_filtrado.rename(columns=rename_dict, inplace=True)
                        if 'CANTIDAD' not in df_filtrado.columns: df_filtrado['CANTIDAD'] = 1
                        
                    elif plataforma == 'TIKTOK':
                        col_order = cols_map.get('order id', cols_map.get('id de pedido'))
                        col_sku = cols_map.get('seller sku', cols_map.get('sku del vendedor'))
                        col_nombre = cols_map.get('product name', cols_map.get('nombre del producto'))
                        col_var = cols_map.get('variation', cols_map.get('variacion'))
                        col_cant = cols_map.get('quantity', cols_map.get('cantidad'))
                        columnas_utiles = [c for c in [col_order, col_sku, col_nombre, col_var, col_cant] if c]
                        df_filtrado = df[columnas_utiles].copy()
                        if col_order: df_filtrado['PEDIDO'] = df_filtrado[col_order].astype(str).str.strip()
                        rename_dict = {}
                        if col_sku: rename_dict[col_sku] = 'SKU'
                        if col_nombre: rename_dict[col_nombre] = 'NOMBRE_ORIGINAL'
                        if col_var: rename_dict[col_var] = 'VARIACION'
                        if col_cant: rename_dict[col_cant] = 'CANTIDAD'
                        df_filtrado.rename(columns=rename_dict, inplace=True)
                        if 'CANTIDAD' not in df_filtrado.columns: df_filtrado['CANTIDAD'] = 1
                        
                    elif plataforma == 'SHEIN':
                        col_pedido = cols_map.get('número de pedido', cols_map.get('numero de pedido'))
                        col_sku = cols_map.get('sku del vendedor')
                        col_nombre = cols_map.get('nombre del producto')
                        col_var = cols_map.get('especificación', cols_map.get('especificacion'))
                        columnas_utiles = [c for c in [col_pedido, col_sku, col_nombre, col_var] if c]
                        df_filtrado = df[columnas_utiles].copy()
                        df_filtrado['CANTIDAD'] = 1
                        if col_pedido: df_filtrado['PEDIDO'] = df_filtrado[col_pedido].astype(str).str.strip()
                        rename_dict = {}
                        if col_sku: rename_dict[col_sku] = 'SKU'
                        if col_nombre: rename_dict[col_nombre] = 'NOMBRE_ORIGINAL'
                        if col_var: rename_dict[col_var] = 'VARIACION'
                        df_filtrado.rename(columns=rename_dict, inplace=True)
                        
                    df_filtrado['PLATAFORMA'] = plataforma
                    df_filtrado['ORDEN_ORIGINAL'] = range(len(df_filtrado))
                    dataframes_limpios.append(df_filtrado)

                if not dataframes_limpios: st.stop()

                # --- UNIFICACIÓN ORIGINAL ---
                df_total = pd.concat(dataframes_limpios, ignore_index=True)
                df_total = df_total.dropna(subset=['PEDIDO'])
                df_total['PEDIDO'] = df_total['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
                df_total['SKU'] = df_total.get('SKU', pd.Series(dtype=str)).fillna('SIN SKU').astype(str)
                df_total['CANTIDAD'] = pd.to_numeric(df_total.get('CANTIDAD', pd.Series(dtype=int)), errors='coerce').fillna(0)
                df_total = df_total[df_total['CANTIDAD'] > 0]
                
                df_total['Nombre Correcto'] = df_total.apply(
                    lambda fila: limpiar_nombre(diccionario_nombres.get(str(fila.get('SKU', '')).strip(), f"{fila.get('NOMBRE_ORIGINAL', '')} - Var: {fila.get('VARIACION', 'N/A')}")), axis=1
                )
                df_total['Nombre Correcto'] = df_total['Nombre Correcto'].fillna('SIN NOMBRE').astype(str)
                df_total['PEDIDO_DISPLAY'] = df_total['PEDIDO']
                
                # --- DETECTAR AVALANCHA VS CARRITO ---
                conteo_por_pedido = df_total.groupby('PEDIDO')['SKU'].nunique().reset_index()
                conteo_por_pedido.columns = ['PEDIDO', 'TIPOS_PRODUCTO']
                df_total = df_total.merge(conteo_por_pedido, on='PEDIDO')
                df_total['TIPO_SURTIDO'] = df_total['TIPOS_PRODUCTO'].apply(lambda x: 'AVALANCHA' if x == 1 else 'CARRITO')
                df_total['TRACKING_ID'] = "" 
                
                # --- REPARTICIÓN EQUITATIVA BASE ---
                pedidos_unicos = list(dict.fromkeys(df_total['PEDIDO'].tolist()))
                num_empleados = len(empleados)
                pos_base = len(pedidos_unicos) // num_empleados
                sobrantes = len(pedidos_unicos) % num_empleados
                cantidades_por_empleado = [pos_base + (1 if i < sobrantes else 0) for i in range(num_empleados)]
                
                asignaciones = {}
                indice_inicio = 0
                for i, emp in enumerate(empleados):
                    indice_fin = indice_inicio + cantidades_por_empleado[i]
                    for po in pedidos_unicos[indice_inicio:indice_fin]: asignaciones[po] = emp
                    indice_inicio = indice_fin
                    
                df_total['ASIGNADO_A'] = df_total['PEDIDO'].map(asignaciones)
                
                st.session_state['master_df'] = df_total
                st.session_state['empleados_activos'] = empleados

                # --- CREACIÓN EXCEL FASE 1 (CON REQUISITOS EXACTOS) ---
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    formato_temu = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                    formato_shein = writer.book.add_format({'bg_color': '#D9EAD3', 'font_color': '#38761D'})
                    formato_tiktok = writer.book.add_format({'bg_color': '#CFE2F3', 'font_color': '#0B5394'})
                    
                    # 1. TOP 5 AVALANCHA (Solo 5 productos)
                    df_ava = df_total[df_total['TIPO_SURTIDO'] == 'AVALANCHA'].groupby(['PLATAFORMA', 'SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
                    df_ava = df_ava.sort_values(by='CANTIDAD', ascending=False).head(5) # EL TOP 5 EXACTO
                    df_ava.to_excel(writer, sheet_name='🔥 TOP 5 AVALANCHA', index=False)
                    ws_ava = writer.sheets['🔥 TOP 5 AVALANCHA']
                    ws_ava.set_column('A:A', 15); ws_ava.set_column('B:B', 20); ws_ava.set_column('C:C', 50); ws_ava.set_column('D:D', 12)
                    
                    # 2. DIVISIONES DE LA AVALANCHA POR EMPLEADO
                    df_ava_div = df_total[df_total['TIPO_SURTIDO'] == 'AVALANCHA'][['ASIGNADO_A', 'PEDIDO', 'PLATAFORMA', 'SKU', 'Nombre Correcto', 'CANTIDAD']]
                    df_ava_div = df_ava_div.sort_values(by=['ASIGNADO_A', 'PLATAFORMA', 'Nombre Correcto'])
                    df_ava_div.rename(columns={'ASIGNADO_A': 'EMPLEADO'}, inplace=True)
                    df_ava_div.to_excel(writer, sheet_name='⚡ ASIGNACION AVALANCHA', index=False)
                    ws_div = writer.sheets['⚡ ASIGNACION AVALANCHA']
                    ws_div.set_column('A:A', 15); ws_div.set_column('B:B', 25); ws_div.set_column('C:C', 15); ws_div.set_column('D:D', 20); ws_div.set_column('E:E', 50)
                    
                    # 3. PICKING DE CARRITOS
                    df_car = df_total[df_total['TIPO_SURTIDO'] == 'CARRITO'].groupby(['PLATAFORMA', 'SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
                    df_car = df_car.sort_values(by=['PLATAFORMA', 'Nombre Correcto'])
                    df_car.to_excel(writer, sheet_name='🛒 PICKING CARRITOS', index=False)
                    ws_car = writer.sheets['🛒 PICKING CARRITOS']
                    ws_car.set_column('A:A', 15); ws_car.set_column('B:B', 20); ws_car.set_column('C:C', 50); ws_car.set_column('D:D', 12)
                    ws_car.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'TEMU', 'format': formato_temu})
                    ws_car.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'SHEIN', 'format': formato_shein})
                    ws_car.conditional_format('A2:A5000', {'type': 'text', 'criteria': 'containing', 'value': 'TIKTOK', 'format': formato_tiktok})

                st.success("✅ ¡Lista de Avalancha (Top 5) y Divisiones guardadas!")
                st.download_button("📥 Descargar Picking Fase 1 (Excel)", data=output.getvalue(), file_name=f"Picking_Almacen_{datetime.now().strftime('%d-%m-%Y')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")

# =====================================================================
# PESTAÑA 2: FASE DE PAQUETERÍA (CÓDIGO ORIGINAL SIN AVALANCHA)
# =====================================================================
with tab_robot:
    st.markdown("### 2. División de Guías y Tickets (Solo Carritos)")
    
    if 'master_df' not in st.session_state:
        st.warning("⚠️ Primero realiza la Fase 1.")
    else:
        df_memoria = st.session_state['master_df']
        empleados_activos = st.session_state['empleados_activos']
        st.success("🧠 Memoria Activa lista. El robot usará tu código original de división y limpiará los tickets de la avalancha.")
        
        col1, col2 = st.columns(2)
        with col1:
            pdf_temu = st.file_uploader("📦 PDF Guías TEMU", type=["pdf"])
            pdf_shein = st.file_uploader("📦 PDF Guías SHEIN", type=["pdf"])
        with col2:
            pdf_tiktok = st.file_uploader("📦 PDF Guías TIKTOK", type=["pdf"])
            tiktok_csv_2 = st.file_uploader("📝 Archivo 2 TIKTOK (CSV con JMX)", type=["csv"])

        if st.button("✂️ Cortar Guías y Crear Tickets", type="primary"):
            if not (pdf_temu or pdf_shein or pdf_tiktok):
                st.error("❌ Sube al menos un PDF.")
                st.stop()
                
            with st.spinner("Emparejando PDFs con tu código original..."):
                paginas_por_pedido = {} 
                
                # --- LÓGICA DE JMX DE TIKTOK ORIGINAL ---
                if pdf_tiktok and tiktok_csv_2:
                    plataforma, codificacion = detectar_plataforma_web(tiktok_csv_2)
                    tiktok_csv_2.seek(0)
                    texto_csv = tiktok_csv_2.getvalue().decode(codificacion)
                    skip_lineas = 0
                    for i, linea in enumerate(texto_csv.splitlines()):
                        lin_low = linea.lower()
                        if ('order id' in lin_low or 'id de pedido' in lin_low):
                            skip_lineas = i; break
                    tiktok_csv_2.seek(0) 
                    df_tk2 = pd.read_csv(tiktok_csv_2, skiprows=skip_lineas, encoding=codificacion)
                    cols_tk = {c.lower().strip(): c for c in df_tk2.columns}
                    
                    col_order = cols_tk.get('order id', cols_tk.get('id de pedido'))
                    col_track = cols_tk.get('tracking id', cols_tk.get('id de seguimiento'))
                    
                    if col_order and col_track:
                        mapa_jmx = {}
                        for idx, row in df_tk2.iterrows():
                            order_id = str(row.get(col_order, '')).replace('.0', '').strip() if col_order else ''
                            tracking_id = str(row.get(col_track, '')).replace('.0', '').strip() if col_track else ''
                            if order_id and order_id != 'nan': mapa_jmx[order_id] = tracking_id
                                
                        df_memoria['TRACKING_ID'] = df_memoria.apply(
                            lambda row: mapa_jmx.get(row['PEDIDO'], "") if row['PLATAFORMA'] == 'TIKTOK' else row['TRACKING_ID'], axis=1
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
                            
                # --- PDF TEMU ORIGINAL ---
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

                # --- PDF SHEIN ORIGINAL ---
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
                # DIVISIÓN NORMAL QUITANDO AVALANCHA DE LOS TICKETS
                # =================================================================
                lista_pos_pdf = list(paginas_por_pedido.keys())
                df_ordenado = df_memoria[df_memoria['PEDIDO'].isin(lista_pos_pdf)].copy()
                
                if df_ordenado.empty:
                    st.error("❌ ERROR: Ningún pedido en el PDF coincidió con la memoria.")
                    st.stop()

                zip_buffer = io.BytesIO()
                colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
                
                with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        
                        for i, emp in enumerate(empleados_activos):
                            df_emp_total = df_ordenado[df_ordenado['ASIGNADO_A'] == emp].copy()
                            
                            # LA CLAVE: Separar Carritos (para Ticket) y Avalancha (solo guías)
                            df_carritos = df_emp_total[df_emp_total['TIPO_SURTIDO'] == 'CARRITO'].copy()
                            df_avalancha = df_emp_total[df_emp_total['TIPO_SURTIDO'] == 'AVALANCHA'].copy()
                            
                            if not df_emp_total.empty:
                                color_actual = colores_division[i % len(colores_division)]
                                
                                # --- 1. TICKET Y EXCEL (SOLO CARRITOS) ---
                                if not df_carritos.empty:
                                    worksheet = writer.book.add_worksheet(emp)
                                    formato_titulo = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': color_actual, 'border': 1})
                                    worksheet.write(0, 0, f"LISTA DE EMPAQUE PARA: {emp.upper()}", formato_titulo)
                                    worksheet.write(1, 0, f"Total de guías mixtas: {df_carritos['PEDIDO'].nunique()}")
                                    
                                    picking_list = df_carritos.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
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
                                    worksheet.write(fila_orden, 0, f"ORDEN EXACTO DE GUÍAS MIXTAS DE {emp.upper()}:", formato_titulo)
                                    df_orden_imp = df_carritos.groupby(['PEDIDO_DISPLAY', 'SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                                    df_orden_imp.rename(columns={'PEDIDO_DISPLAY': 'PEDIDO', 'CANTIDAD': 'Cant.'}, inplace=True)
                                    df_orden_imp.to_excel(writer, sheet_name=emp, index=False, startrow=fila_orden + 2, startcol=0)
                                    
                                    # HOJA TICKET TÉRMICO
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

                                # --- 2. PDFs SEPARADOS (PARA QUE SEA FÁCIL EMPACAR) ---
                                # PDF 1: Avalancha (Sin ticket, solo hojas para pegar rápido)
                                if not df_avalancha.empty:
                                    pdf_ava_writer = PyPDF2.PdfWriter()
                                    for po in df_avalancha['PEDIDO'].unique():
                                        if po in paginas_por_pedido:
                                            for p in paginas_por_pedido[po]: pdf_ava_writer.add_page(p)
                                    pdf_ava_buf = io.BytesIO()
                                    pdf_ava_writer.write(pdf_ava_buf)
                                    zip_file.writestr(f"1_Avalancha_{emp}.pdf", pdf_ava_buf.getvalue())
                                
                                # PDF 2: Carritos (Para usar con el Ticket térmico)
                                if not df_carritos.empty:
                                    pdf_car_writer = PyPDF2.PdfWriter()
                                    for po in df_carritos['PEDIDO'].unique():
                                        if po in paginas_por_pedido:
                                            for p in paginas_por_pedido[po]: pdf_car_writer.add_page(p)
                                    pdf_car_buf = io.BytesIO()
                                    pdf_car_writer.write(pdf_car_buf)
                                    zip_file.writestr(f"2_Carritos_{emp}.pdf", pdf_car_buf.getvalue())

                    zip_file.writestr("Tickets_Carritos.xlsx", excel_buffer.getvalue())
                st.session_state['descarga_pdfs'] = zip_buffer.getvalue()

        if 'descarga_pdfs' in st.session_state:
            st.balloons()
            st.success("✂️ ¡División lista! La Avalancha se quitó de los tickets térmicos.")
            st.download_button(
                label="📦 Descargar ZIP (Tickets Limpios y PDFs)",
                data=st.session_state['descarga_pdfs'],
                file_name=f"Guias_Tickets_Vmingo_{datetime.now().strftime('%d-%m-%Y')}.zip",
                mime="application/zip",
                type="primary"
            )
