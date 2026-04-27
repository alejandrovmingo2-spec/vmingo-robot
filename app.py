import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Vmingo ERP - Robot Almacén", page_icon="🤖", layout="wide")

# =====================================================================
# FUNCIONES ORIGINALES BILINGÜES A PRUEBA DE BALAS
# =====================================================================
def limpiar_nombre(texto):
    idx = texto.lower().find('detalle')
    if idx != -1: return texto[:idx].strip()
    return texto.strip()

def detectar_plataforma_csv(archivo_csv_buffer):
    encodings_a_probar = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252']
    contenido = archivo_csv_buffer.getvalue()
    for cod in encodings_a_probar:
        try:
            texto = contenido.decode(cod)
            lineas = texto.splitlines()
            for linea in lineas[:5]:
                lin_low = linea.lower()
                if 'id del pedido' in lin_low and 'sku de contribución' in lin_low: return 'TEMU', cod
                if ('order id' in lin_low or 'id de pedido' in lin_low) and ('seller sku' in lin_low or 'sku del vendedor' in lin_low): return 'TIKTOK', cod
                if 'número de pedido' in lin_low and 'sku del vendedor' in lin_low: return 'SHEIN', cod
        except: pass
    return 'DESCONOCIDA', None

def procesar_csv(archivo, plataforma, codificacion):
    archivo.seek(0)
    texto_csv = archivo.getvalue().decode(codificacion)
    skip_lineas = 0
    for i, linea in enumerate(texto_csv.splitlines()):
        lin_low = linea.lower()
        if (plataforma == 'TEMU' and 'id del pedido' in lin_low) or \
           (plataforma == 'TIKTOK' and ('order id' in lin_low or 'id de pedido' in lin_low)) or \
           (plataforma == 'SHEIN' and 'número de pedido' in lin_low):
            skip_lineas = i; break
            
    archivo.seek(0) 
    df = pd.read_csv(archivo, skiprows=skip_lineas, encoding=codificacion)
    cols_map = {c.lower().strip(): c for c in df.columns}

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
        col_track = cols_map.get('tracking id', cols_map.get('id de seguimiento'))
        
        columnas_utiles = [c for c in [col_order, col_sku, col_nombre, col_var, col_cant, col_track] if c]
        df_filtrado = df[columnas_utiles].copy()
        if col_order: df_filtrado['PEDIDO'] = df_filtrado[col_order].astype(str).str.strip()
        if col_track: df_filtrado['TRACKING_ID'] = df_filtrado[col_track].astype(str).str.strip()
        
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
    if 'TRACKING_ID' not in df_filtrado.columns: df_filtrado['TRACKING_ID'] = ""
    return df_filtrado

def unificar_y_matematicas(dataframes, empleados, diccionario_nombres):
    df_total = pd.concat(dataframes, ignore_index=True)
    df_total = df_total.dropna(subset=['PEDIDO'])
    df_total['PEDIDO'] = df_total['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
    df_total['SKU'] = df_total.get('SKU', pd.Series(dtype=str)).fillna('SIN SKU').astype(str)
    
    # Filtro mágico antibasura
    df_total['CANTIDAD'] = pd.to_numeric(df_total.get('CANTIDAD', pd.Series(dtype=int)), errors='coerce').fillna(0)
    df_total = df_total[df_total['CANTIDAD'] > 0]
    
    df_total['Nombre Correcto'] = df_total.apply(
        lambda fila: limpiar_nombre(diccionario_nombres.get(str(fila.get('SKU', '')).strip(), f"{fila.get('NOMBRE_ORIGINAL', '')} - Var: {fila.get('VARIACION', 'N/A')}")), axis=1
    )
    df_total['Nombre Correcto'] = df_total['Nombre Correcto'].fillna('SIN NOMBRE').astype(str)
    df_total['PEDIDO_DISPLAY'] = df_total['PEDIDO']
    
    # 1. CONTAR PRODUCTOS POR PEDIDO
    conteo_por_pedido = df_total.groupby('PEDIDO')['SKU'].nunique().reset_index()
    conteo_por_pedido.columns = ['PEDIDO', 'TIPOS_PRODUCTO']
    df_total = df_total.merge(conteo_por_pedido, on='PEDIDO')
    
    # 2. DEFINIR EL TOP 5 AVALANCHA EXACTO
    df_single = df_total[df_total['TIPOS_PRODUCTO'] == 1]
    top_5_skus = df_single.groupby('SKU')['CANTIDAD'].sum().nlargest(5).index.tolist()
    
    df_total['TIPO_SURTIDO'] = df_total.apply(
        lambda row: 'AVALANCHA' if (row['TIPOS_PRODUCTO'] == 1 and row['SKU'] in top_5_skus) else 'CARRITO', axis=1
    )
    
    # 3. REPARTICIÓN EQUITATIVA EXACTA
    num_emp = len(empleados)
    asignaciones = {}
    
    # Repartir Avalancha
    pedidos_ava = df_total[df_total['TIPO_SURTIDO'] == 'AVALANCHA']['PEDIDO'].unique()
    base_ava = len(pedidos_ava) // num_emp
    sob_ava = len(pedidos_ava) % num_emp
    cant_ava = [base_ava + (1 if i < sob_ava else 0) for i in range(num_emp)]
    idx = 0
    for i, emp in enumerate(empleados):
        fin = idx + cant_ava[i]
        for po in pedidos_ava[idx:fin]: asignaciones[po] = emp
        idx = fin

    # Repartir Carritos
    pedidos_car = df_total[df_total['TIPO_SURTIDO'] == 'CARRITO']['PEDIDO'].unique()
    base_car = len(pedidos_car) // num_emp
    sob_car = len(pedidos_car) % num_emp
    cant_car = [base_car + (1 if i < sob_car else 0) for i in range(num_emp)]
    idx = 0
    for i, emp in enumerate(empleados):
        fin = idx + cant_car[i]
        for po in pedidos_car[idx:fin]: asignaciones[po] = emp
        idx = fin
        
    df_total['ASIGNADO_A'] = df_total['PEDIDO'].map(asignaciones)
    return df_total

st.title("🤖 Vmingo ERP: Centro de Surtido y Empaque")

tab_picking, tab_robot = st.tabs(["🛒 FASE 1: Picking (Surtido)", "📦 FASE 2: Emparejador y Tickets (Independiente)"])

# =====================================================================
# PESTAÑA 1: FASE DE ALMACÉN
# =====================================================================
with tab_picking:
    st.markdown("### 1. Extracción de Listas de Recolección")
    st.info("Sube los documentos CSV de las tiendas. El robot detectará el Top 5 Avalancha y asignará los Carritos.")
    
    col_t, col_s, col_k = st.columns(3)
    with col_t: file_temu1 = st.file_uploader("A. CSV TEMU", type=["csv"], key="t_temu1")
    with col_s: file_shein1 = st.file_uploader("B. CSV SHEIN", type=["csv"], key="t_shein1")
    with col_k: file_tiktok1 = st.file_uploader("C. CSV TIKTOK", type=["csv"], key="t_tiktok1")
        
    col_base, col_emp = st.columns([1, 2])
    with col_base: base_picking1 = st.file_uploader("D. BASE (Opcional)", type=["xlsx", "xlsm"], key="base_pick1")
    with col_emp: emp_input1 = st.text_input("Nombres del equipo:", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR", key="emp1")

    if st.button("📊 Generar Listas de Picking", type="primary"):
        archivos = [f for f in [file_temu1, file_shein1, file_tiktok1] if f is not None]
        empleados = [e.strip().upper() for e in emp_input1.split(',') if e.strip()]
        
        if not archivos: st.error("❌ Sube al menos un archivo CSV.")
        elif not empleados: st.error("❌ Necesitas ingresar al menos un nombre.")
        else:
            with st.spinner("Analizando CSVs con la lógica original..."):
                dicc = {}
                if base_picking1:
                    try:
                        df_base = pd.read_excel(base_picking1, sheet_name='BASE')
                        for idx, fila in df_base.iterrows():
                            sku = str(fila.get('SKU', '')).strip()
                            nom = str(fila.get('NOMBRE PLATAFORMA', '')).strip()
                            if sku and sku != 'nan': dicc[sku] = nom
                    except: pass

                dataframes = []
                for arch in archivos:
                    plat, cod = detectar_plataforma_csv(arch)
                    if plat != 'DESCONOCIDA': dataframes.append(procesar_csv(arch, plat, cod))
                    else: st.error(f"❌ ERROR: No reconocí {arch.name}.")

                if dataframes:
                    df_final = unificar_y_matematicas(dataframes, empleados, dicc)
                    
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        # 1. TOP 5 AVALANCHA (Resumen)
                        df_ava = df_final[df_final['TIPO_SURTIDO'] == 'AVALANCHA'].groupby(['PLATAFORMA', 'SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
                        df_ava = df_ava.sort_values(by='CANTIDAD', ascending=False)
                        df_ava.to_excel(writer, sheet_name='🔥 TOP 5 AVALANCHA', index=False)
                        ws_ava = writer.sheets['🔥 TOP 5 AVALANCHA']
                        ws_ava.set_column('A:A', 15); ws_ava.set_column('B:B', 20); ws_ava.set_column('C:C', 50); ws_ava.set_column('D:D', 12)
                        
                        # 2. ASIGNACIÓN AVALANCHA EXACTA
                        df_ava_asig = df_final[df_final['TIPO_SURTIDO'] == 'AVALANCHA'][['ASIGNADO_A', 'PEDIDO', 'PLATAFORMA', 'SKU', 'Nombre Correcto', 'CANTIDAD']]
                        df_ava_asig = df_ava_asig.sort_values(by=['ASIGNADO_A', 'PLATAFORMA'])
                        df_ava_asig.to_excel(writer, sheet_name='⚡ ASIGNACION AVALANCHA', index=False)
                        ws_asig = writer.sheets['⚡ ASIGNACION AVALANCHA']
                        ws_asig.set_column('A:A', 15); ws_asig.set_column('B:B', 25); ws_asig.set_column('C:C', 15); ws_asig.set_column('D:D', 20); ws_asig.set_column('E:E', 50)
                        
                        # 3. LISTAS DE CARRITOS POR EMPLEADO
                        for emp in empleados:
                            df_car = df_final[(df_final['ASIGNADO_A'] == emp) & (df_final['TIPO_SURTIDO'] == 'CARRITO')].groupby(['PLATAFORMA', 'SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
                            if not df_car.empty:
                                df_car = df_car.sort_values(by=['PLATAFORMA', 'Nombre Correcto'])
                                df_car.to_excel(writer, sheet_name=f"🛒 PICKING {emp}", index=False)
                                ws_car = writer.sheets[f"🛒 PICKING {emp}"]
                                ws_car.set_column('A:A', 15); ws_car.set_column('B:B', 20); ws_car.set_column('C:C', 50); ws_car.set_column('D:D', 12)

                    st.success("✅ ¡Lista de Avalancha (Top 5 exacto) y Pickings individuales creados!")
                    st.download_button("📥 Descargar Picking Fase 1 (Excel)", data=output.getvalue(), file_name=f"Picking_Almacen_{datetime.now().strftime('%d-%m-%Y')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")

# =====================================================================
# PESTAÑA 2: FASE DE PAQUETERÍA (INDEPENDIENTE)
# =====================================================================
with tab_robot:
    st.markdown("### 2. División de Guías y Tickets (Módulos Independientes)")
    st.info("💡 **Para que los Tickets cuadren con el almacén:** Sube los mismos 3 CSVs de la mañana para calcular la matemática, y a lado sube los PDFs de las guías que ya tengas listas.")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write("🔴 **TEMU**")
        csv_t2 = st.file_uploader("CSV Temu", type=["csv"], key="c_t2")
        pdf_t2 = st.file_uploader("Guías Temu (PDF)", type=["pdf"], key="p_t2")
    with col2:
        st.write("🟢 **SHEIN**")
        csv_s2 = st.file_uploader("CSV Shein", type=["csv"], key="c_s2")
        pdf_s2 = st.file_uploader("Guías Shein (PDF)", type=["pdf"], key="p_s2")
    with col3:
        st.write("🔵 **TIKTOK**")
        csv_k2 = st.file_uploader("CSV TikTok", type=["csv"], key="c_k2")
        pdf_k2 = st.file_uploader("Guías TikTok (PDF)", type=["pdf"], key="p_k2")

    col_base2, col_emp2 = st.columns([1, 2])
    with col_base2: base_pick2 = st.file_uploader("BASE (Opcional)", type=["xlsx", "xlsm"], key="base2")
    with col_emp2: emp_input2 = st.text_input("Nombres del equipo:", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR", key="emp2")

    if st.button("✂️ Cortar Guías y Crear Tickets", type="primary"):
        csvs_subidos = [f for f in [csv_t2, csv_s2, csv_k2] if f is not None]
        empleados2 = [e.strip().upper() for e in emp_input2.split(',') if e.strip()]
        
        if not csvs_subidos: st.error("❌ Sube al menos un CSV para armar la matemática.")
        elif not (pdf_t2 or pdf_s2 or pdf_k2): st.error("❌ Sube al menos un PDF de guías para cortar.")
        else:
            with st.spinner("Reconstruyendo asignaciones y emparejando PDFs..."):
                dicc2 = {}
                if base_pick2:
                    try:
                        df_base = pd.read_excel(base_pick2, sheet_name='BASE')
                        for idx, fila in df_base.iterrows():
                            sku = str(fila.get('SKU', '')).strip()
                            nom = str(fila.get('NOMBRE PLATAFORMA', '')).strip()
                            if sku and sku != 'nan': dicc2[sku] = nom
                    except: pass

                dfs2 = []
                for arch in csvs_subidos:
                    plat, cod = detectar_plataforma_csv(arch)
                    if plat != 'DESCONOCIDA': dfs2.append(procesar_csv(arch, plat, cod))
                
                # RECREAMOS LA MATEMÁTICA EXACTA
                df_matriz = unificar_y_matematicas(dfs2, empleados2, dicc2)
                paginas_por_pedido = {} 

                # MATCH TIKTOK
                if pdf_k2 and csv_k2:
                    df_tk_memoria = df_matriz[df_matriz['PLATAFORMA'] == 'TIKTOK']
                    reader_tk = PyPDF2.PdfReader(pdf_k2)
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
                    
                    for idx, row in df_tk_memoria.iterrows():
                        jmx = str(row['TRACKING_ID']).replace('.0', '').strip()
                        pedido = row['PEDIDO']
                        if jmx in temp_jmx_pages: paginas_por_pedido[pedido] = temp_jmx_pages[jmx]

                # MATCH TEMU
                if pdf_t2:
                    reader_temu = PyPDF2.PdfReader(pdf_t2)
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

                # MATCH SHEIN
                if pdf_s2:
                    reader_shein = PyPDF2.PdfReader(pdf_s2)
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
                    
                    pedidos_shein = df_matriz[df_matriz['PLATAFORMA'] == 'SHEIN'].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique()
                    for i, ped_shein in enumerate(pedidos_shein):
                        if i < len(chunks_shein): paginas_por_pedido[ped_shein] = chunks_shein[i]

                # =================================================================
                # EXCEL Y PDFs FINALES (SIN AVALANCHA EN TICKETS)
                # =================================================================
                # Filtramos la matriz solo a los pedidos que sí encontramos en PDF
                lista_pos_pdf = list(paginas_por_pedido.keys())
                df_impresion = df_matriz[df_matriz['PEDIDO'].isin(lista_pos_pdf)].copy()
                
                if df_impresion.empty:
                    st.error("❌ ERROR: Ninguna guía del PDF cruzó con los CSV.")
                    st.stop()
                    
                df_impresion['PEDIDO'] = pd.Categorical(df_impresion['PEDIDO'], categories=lista_pos_pdf, ordered=True)
                df_impresion = df_impresion.sort_values('PEDIDO')

                zip_buffer = io.BytesIO()
                colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
                
                with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        
                        for i, emp in enumerate(empleados2):
                            df_emp_todo = df_impresion[df_impresion['ASIGNADO_A'] == emp].copy()
                            df_carritos = df_emp_todo[df_emp_todo['TIPO_SURTIDO'] == 'CARRITO'].copy()
                            df_avalancha = df_emp_todo[df_emp_todo['TIPO_SURTIDO'] == 'AVALANCHA'].copy()
                            
                            color_actual = colores_division[i % len(colores_division)]
                            
                            # 1. TICKET Y LISTA (SOLO CARRITOS)
                            if not df_carritos.empty:
                                pos_carritos = df_carritos['PEDIDO'].unique()
                                worksheet = writer.book.add_worksheet(emp)
                                formato_titulo = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': color_actual, 'border': 1})
                                worksheet.write(0, 0, f"LISTA DE EMPAQUE CARRITO: {emp.upper()}", formato_titulo)
                                worksheet.write(1, 0, f"Total de guías en carrito: {len(pos_carritos)}")
                                
                                picking_list = df_carritos.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                                picking_list.rename(columns={'Nombre Correcto': 'Descripción', 'CANTIDAD': 'Total a Empacar'}, inplace=True)
                                picking_list = picking_list.sort_values(by='Descripción').reset_index(drop=True)

                                inicio_t1 = 3; fin_t1 = inicio_t1 + len(picking_list)
                                picking_list.to_excel(writer, sheet_name=emp, index=False, header=False, startrow=inicio_t1 + 1, startcol=0)
                                worksheet.add_table(inicio_t1, 0, fin_t1, len(picking_list.columns) - 1, {'columns': [{'header': col} for col in picking_list.columns], 'style': 'Table Style Medium 9'})
                                worksheet.set_column('A:A', 20); worksheet.set_column('B:B', 65)

                                fila_orden = fin_t1 + 3
                                worksheet.write(fila_orden, 0, f"ORDEN EXACTO DE GUÍAS DE {emp.upper()}:", formato_titulo)
                                df_orden_imp = df_carritos.groupby(['PEDIDO_DISPLAY', 'SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
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
                                
                                hoja_ticket.write('A1', f'DIVISION {i+1}', fmt_header); hoja_ticket.write('D1', 'CARRITO', fmt_header) 
                                hoja_ticket.merge_range('A2:D2', emp.upper(), fmt_titulo_ticket)
                                
                                encabezados = ['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']
                                for col, encabezado in enumerate(encabezados):
                                    if encabezado == 'CANTI\nDAD': hoja_ticket.write(3, col, encabezado, fmt_wrap)
                                    else: hoja_ticket.write(3, col, encabezado, fmt_header)
                                    
                                total_piezas = 0; fila = 4
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

                                # PDF CARRITOS
                                pdf_car_writer = PyPDF2.PdfWriter()
                                for po in pos_carritos:
                                    if po in paginas_por_pedido:
                                        for p in paginas_por_pedido[po]: pdf_car_writer.add_page(p)
                                pdf_car_buf = io.BytesIO()
                                pdf_car_writer.write(pdf_car_buf)
                                zip_file.writestr(f"2_CARRITO_{emp}.pdf", pdf_car_buf.getvalue())

                            # 2. PDF AVALANCHA (SIN TICKET)
                            if not df_avalancha.empty:
                                pos_ava = df_avalancha['PEDIDO'].unique()
                                pdf_ava_writer = PyPDF2.PdfWriter()
                                for po in pos_ava:
                                    if po in paginas_por_pedido:
                                        for p in paginas_por_pedido[po]: pdf_ava_writer.add_page(p)
                                pdf_ava_buf = io.BytesIO()
                                pdf_ava_writer.write(pdf_ava_buf)
                                zip_file.writestr(f"1_AVALANCHA_{emp}.pdf", pdf_ava_buf.getvalue())

                    zip_file.writestr("Tickets_Solo_Carritos.xlsx", excel_buffer.getvalue())
                st.session_state['descarga_pdfs'] = zip_buffer.getvalue()

        if 'descarga_pdfs' in st.session_state:
            st.balloons()
            st.success("✂️ ¡Tickets limpios y PDFs separados listos!")
            st.download_button(
                label="📦 Descargar ZIP (Tickets + Guías)",
                data=st.session_state['descarga_pdfs'],
                file_name=f"Guias_Tickets_Vmingo_{datetime.now().strftime('%d-%m-%Y')}.zip",
                mime="application/zip",
                type="primary"
            )
