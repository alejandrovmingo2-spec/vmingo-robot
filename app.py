import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Vmingo ERP - Robot Almacén", page_icon="🤖", layout="wide")

# =====================================================================
# FUNCIONES MATEMÁTICAS Y DE EXTRACCIÓN
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
                if ('order id' in lin_low or 'id de pedido' in lin_low) and ('seller sku' in lin_low or 'sku del vendedor' in lin_low or 'tracking id' in lin_low or 'id de seguimiento' in lin_low): return 'TIKTOK', cod
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
        col_nom = cols_map.get('nombre del producto')
        col_var = cols_map.get('variación', cols_map.get('variacion'))
        col_cant = cols_map.get('cantidad a enviar')
        df_f = df[[c for c in [col_pedido, col_sku, col_nom, col_var, col_cant] if c]].copy()
        df_f.rename(columns={col_pedido:'PEDIDO', col_sku:'SKU', col_nom:'NOMBRE_ORIGINAL', col_var:'VARIACION', col_cant:'CANTIDAD'}, inplace=True)
        
    elif plataforma == 'TIKTOK':
        col_order = cols_map.get('order id', cols_map.get('id de pedido'))
        col_sku = cols_map.get('seller sku', cols_map.get('sku del vendedor'))
        col_nom = cols_map.get('product name', cols_map.get('nombre del producto'))
        col_var = cols_map.get('variation', cols_map.get('variacion'))
        col_cant = cols_map.get('quantity', cols_map.get('cantidad'))
        col_track = cols_map.get('tracking id', cols_map.get('id de seguimiento'))
        df_f = df[[c for c in [col_order, col_sku, col_nom, col_var, col_cant, col_track] if c]].copy()
        df_f.rename(columns={col_order:'PEDIDO', col_sku:'SKU', col_nom:'NOMBRE_ORIGINAL', col_var:'VARIACION', col_cant:'CANTIDAD', col_track:'TRACKING_ID'}, inplace=True)
        
    elif plataforma == 'SHEIN':
        col_pedido = cols_map.get('número de pedido', cols_map.get('numero de pedido'))
        col_sku = cols_map.get('sku del vendedor')
        col_nom = cols_map.get('nombre del producto')
        col_var = cols_map.get('especificación', cols_map.get('especificacion'))
        df_f = df[[c for c in [col_pedido, col_sku, col_nom, col_var] if c]].copy()
        df_f.rename(columns={col_pedido:'PEDIDO', col_sku:'SKU', col_nom:'NOMBRE_ORIGINAL', col_var:'VARIACION'}, inplace=True)
        df_f['CANTIDAD'] = 1
        
    df_f['PLATAFORMA'] = plataforma
    df_f['ORDEN_ORIGINAL'] = range(len(df_f)) 
    if 'TRACKING_ID' not in df_f.columns: df_f['TRACKING_ID'] = ""
    return df_f

def unificar_y_distribuir(dataframes, empleados, dicc_nombres, dicc_tipos, limite_cajas=15):
    df_total = pd.concat(dataframes, ignore_index=True)
    df_total['PEDIDO'] = df_total['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
    df_total['SKU'] = df_total['SKU'].astype(str).str.strip()
    df_total['CANTIDAD'] = pd.to_numeric(df_total['CANTIDAD'], errors='coerce').fillna(1)
    df_total = df_total[df_total['CANTIDAD'] > 0] 
    
    df_total['Nombre Correcto'] = df_total['SKU'].apply(lambda x: limpiar_nombre(dicc_nombres.get(x, "SIN NOMBRE EN BASE")))
    df_total['TIPO'] = df_total['SKU'].apply(lambda x: dicc_tipos.get(x, "NORMAL"))
    
    conteo_pedidos = df_total.groupby('PEDIDO')['SKU'].nunique().reset_index()
    conteo_pedidos.columns = ['PEDIDO', 'TIPOS_PRODUCTO']
    df_total = df_total.merge(conteo_pedidos, on='PEDIDO')
    
    df_single = df_total[df_total['TIPOS_PRODUCTO'] == 1]
    top_5_skus = df_single.groupby('SKU')['CANTIDAD'].sum().nlargest(5).index.tolist()
    
    asignaciones = {}
    emp_idx = 0
    num_emp = len(empleados)
    
    # LEY 1: AVALANCHA TOP 5
    pedidos_ava = df_single[df_single['SKU'].isin(top_5_skus)]['PEDIDO'].unique().tolist()
    pedidos_ava.sort()
    for p in pedidos_ava:
        asignaciones[p] = empleados[emp_idx % num_emp]
        emp_idx += 1

    # LEY 2: MINI-AVALANCHAS "CAJA"
    df_mini_caja = df_single[(~df_single['SKU'].isin(top_5_skus)) & (df_single['TIPO'] == 'CAJA')]
    sku_vol_caja = df_mini_caja.groupby('SKU')['CANTIDAD'].sum().sort_values(ascending=False).index.tolist()
    for sku in sku_vol_caja:
        peds_sku = df_mini_caja[df_mini_caja['SKU'] == sku]['PEDIDO'].unique().tolist()
        peds_sku.sort()
        for i in range(0, len(peds_sku), limite_cajas):
            chunk = peds_sku[i : i + limite_cajas]
            emp_asignado = empleados[emp_idx % num_emp]
            for p in chunk: asignaciones[p] = emp_asignado
            emp_idx += 1

    # LEY 3: MINI-AVALANCHAS NORMALES
    df_mini_norm = df_single[(~df_single['SKU'].isin(top_5_skus)) & (df_single['TIPO'] != 'CAJA')]
    sku_vol_norm = df_mini_norm.groupby('SKU')['CANTIDAD'].sum().sort_values(ascending=False).index.tolist()
    for sku in sku_vol_norm:
        peds_sku = df_mini_norm[df_mini_norm['SKU'] == sku]['PEDIDO'].unique().tolist()
        emp_asignado = empleados[emp_idx % num_emp] 
        for p in peds_sku: asignaciones[p] = emp_asignado
        emp_idx += 1

    # LEY 4: CARRITOS MIXTOS
    df_mixtos = df_total[df_total['TIPOS_PRODUCTO'] > 1]
    pedidos_mixtos = df_mixtos['PEDIDO'].unique().tolist()
    pedidos_mixtos.sort()
    for p in pedidos_mixtos:
        asignaciones[p] = empleados[emp_idx % num_emp]
        emp_idx += 1
                
    df_total['ASIGNADO_A'] = df_total['PEDIDO'].map(asignaciones)

    df_total['CATEGORIA'] = df_total.apply(
        lambda r: 'AVALANCHA' if (r['TIPOS_PRODUCTO'] == 1 and r['SKU'] in top_5_skus) else 'CARRITO', axis=1
    )

    return df_total, top_5_skus

st.title("🤖 Vmingo ERP: Coordinador Logístico Multiplataforma")

tab1, tab2 = st.tabs(["🛒 FASE 1: Picking (Surtido de Almacén)", "📦 FASE 2: Empaque (Cruce PDF y Tickets)"])

# =====================================================================
# FASE 1: LISTAS DE RECOLECCIÓN
# =====================================================================
with tab1:
    st.markdown("### 1. Planificación de Trabajo Matutino")
    col_t, col_s, col_k = st.columns(3)
    with col_t: f_t = st.file_uploader("CSV TEMU", type=["csv"], key="t1")
    with col_s: f_s = st.file_uploader("CSV SHEIN", type=["csv"], key="s1")
    with col_k: f_k = st.file_uploader("CSV TIKTOK", type=["csv"], key="k1")
    
    col_b, col_e = st.columns([1, 2])
    with col_b: f_base = st.file_uploader("BASE (Con columna TIPO)", type=["xlsx", "xlsm"], key="b1")
    with col_e: e_in = st.text_input("Equipo en Turno (Separado por comas):", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR")

    if st.button("📊 Generar Mega-Picking Equitativo", type="primary"):
        archs = [f for f in [f_t, f_s, f_k] if f is not None]
        
        # BLINDAJE ANTI-DUPLICADOS (Quita nombres repetidos y espacios)
        raw_emps = [e.strip().upper() for e in e_in.split(',') if e.strip()]
        emps = list(dict.fromkeys(raw_emps)) # Mantiene el orden pero elimina los clonados
        
        if not archs or not emps: st.error("Faltan datos. Sube CSVs y escribe los nombres.")
        else:
            with st.spinner("Licuando datos bajo las 4 Leyes Logísticas..."):
                dicc_nom, dicc_tipo = {}, {}
                if f_base:
                    try:
                        df_b = pd.read_excel(f_base, sheet_name='BASE')
                    except Exception:
                        df_b = pd.read_excel(f_base)
                    df_b.columns = df_b.columns.str.strip().str.upper()
                    for _, r in df_b.iterrows():
                        s = str(r.get('SKU','')).strip()
                        if s and s != 'nan':
                            dicc_nom[s] = str(r.get('NOMBRE PLATAFORMA','')).strip()
                            dicc_tipo[s] = str(r.get('TIPO','NORMAL')).strip().upper()

                dfs = [procesar_csv(a, detectar_plataforma_csv(a)[0], detectar_plataforma_csv(a)[1]) for a in archs]
                df_final, top5 = unificar_y_distribuir(dfs, emps, dicc_nom, dicc_tipo, limite_cajas=15)
                
                total_pedidos = df_final['PEDIDO'].nunique()
                total_piezas = int(df_final['CANTIDAD'].sum())
                
                m1, m2 = st.columns(2)
                m1.metric("📦 Total de Pedidos del Día", total_pedidos)
                m2.metric("🧩 Total de Piezas Físicas (Volumen)", total_piezas)
                
                output = io.BytesIO()
                colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
                
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_resumen = df_final[df_final['CATEGORIA'] == 'AVALANCHA'].groupby(['SKU','Nombre Correcto','TIPO'])['CANTIDAD'].sum().reset_index()
                    df_resumen.sort_values(by='CANTIDAD', ascending=False).to_excel(writer, sheet_name='🔥 TOP 5 AVALANCHA', index=False)
                    ws_ava = writer.sheets['🔥 TOP 5 AVALANCHA']
                    ws_ava.set_column('A:A', 15); ws_ava.set_column('B:B', 60); ws_ava.set_column('C:C', 15); ws_ava.set_column('D:D', 15)

                    df_asig_ava = df_final[df_final['CATEGORIA'] == 'AVALANCHA'].groupby(['ASIGNADO_A', 'SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
                    if not df_asig_ava.empty:
                        df_asig_ava = df_asig_ava.sort_values(by=['ASIGNADO_A', 'CANTIDAD'], ascending=[True, False])
                        df_asig_ava.rename(columns={'ASIGNADO_A': 'EMPLEADO'}, inplace=True)
                        df_asig_ava.to_excel(writer, sheet_name='⚡ ASIGNACION AVALANCHA', index=False)
                        ws_asig = writer.sheets['⚡ ASIGNACION AVALANCHA']
                        ws_asig.set_column('A:A', 20); ws_asig.set_column('B:B', 20); ws_asig.set_column('C:C', 60); ws_asig.set_column('D:D', 15)
                    
                    for i, e in enumerate(emps):
                        df_e = df_final[(df_final['ASIGNADO_A'] == e) & (df_final['CATEGORIA'] == 'CARRITO')].groupby(['SKU','Nombre Correcto','TIPO'])['CANTIDAD'].sum().reset_index()
                        if not df_e.empty:
                            df_e = df_e.sort_values(by='Nombre Correcto').reset_index(drop=True)
                            color_actual = colores_division[i % len(colores_division)]
                            
                            hoja_ticket = writer.book.add_worksheet(f"🛒 {e}_Ticket")
                            fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                            fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                            fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                            fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                            fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                            fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                            
                            hoja_ticket.write('A1', f'DIVISION {i+1}', fmt_header)
                            hoja_ticket.write('D1', 'CARRITO', fmt_header) 
                            hoja_ticket.merge_range('A2:D2', f"SURTIR: {e.upper()}", fmt_titulo_ticket)
                            
                            encabezados = ['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']
                            for col, encab in enumerate(encabezados):
                                if encab == 'CANTI\nDAD': hoja_ticket.write(3, col, encab, fmt_wrap)
                                else: hoja_ticket.write(3, col, encab, fmt_header)
                                
                            total_piezas_emp = 0
                            fila = 4
                            for idx, item in df_e.iterrows():
                                cant = int(item['CANTIDAD'])
                                total_piezas_emp += cant
                                hoja_ticket.write(fila, 0, idx + 1, fmt_td_centro) 
                                hoja_ticket.write(fila, 1, item['SKU'], fmt_td_centro)            
                                hoja_ticket.write(fila, 2, item['Nombre Correcto'], fmt_td_izq)  
                                hoja_ticket.write(fila, 3, cant, fmt_td_centro)     
                                fila += 1
                                
                            hoja_ticket.write(fila, 0, len(df_e) + 1, fmt_td_centro)
                            hoja_ticket.merge_range(fila, 1, fila, 2, 'Total de Carrito', fmt_total)
                            hoja_ticket.write(fila, 3, total_piezas_emp, fmt_total)
                            
                            hoja_ticket.set_column('A:A', 4); hoja_ticket.set_column('B:B', 16); hoja_ticket.set_column('C:C', 38); hoja_ticket.set_column('D:D', 6)
                            hoja_ticket.set_row(3, 30); hoja_ticket.set_row(1, 25) 
                            hoja_ticket.fit_to_pages(1, 0); hoja_ticket.set_margins(left=0.1, right=0.1, top=0.1, bottom=0.1)
                
                st.success("✅ ¡Tickets generados listos para imprimir! Los duplicados en la lista de nombres fueron ignorados.")
                st.download_button("📥 Descargar Picking Fase 1", output.getvalue(), f"Picking_Termico_{datetime.now().strftime('%d-%m-%Y')}.xlsx", "application/vnd.ms-excel")

# =====================================================================
# FASE 2: EMPAQUE Y MULTI-PDF 
# =====================================================================
with tab2:
    st.markdown("### 2. Generador de Guías y Tickets de Empaque")
    st.info("💡 Sube los MISMOS CSVs de la mañana para cuadrar matemáticas. Aparte, sube el JMX y los PDFs para las guías.")
    
    col_t2, col_s2, col_k2 = st.columns(3)
    with col_t2:
        st.write("🔴 **TEMU**")
        csv_t2 = st.file_uploader("CSV Temu (Mismo de F1)", type=["csv"], key="ct2")
        pdf_t2 = st.file_uploader("PDF Temu", type=["pdf"], key="pt2")
    with col_s2:
        st.write("🟢 **SHEIN**")
        csv_s2 = st.file_uploader("CSV Shein (Mismo de F1)", type=["csv"], key="cs2")
        pdf_s2 = st.file_uploader("PDF Shein", type=["pdf"], key="ps2")
    with col_k2:
        st.write("🔵 **TIKTOK**")
        csv_k2 = st.file_uploader("CSV TikTok (Mismo de F1)", type=["csv"], key="ck2")
        csv_jmx = st.file_uploader("CSV TikTok JMX (Opcional)", type=["csv"], key="cjmx")
        pdf_k2 = st.file_uploader("PDF TikTok", type=["pdf"], key="pk2")
    
    f_base2 = st.file_uploader("BASE", type=["xlsx", "xlsm"], key="b2")

    if st.button("✂️ Cortar y Generar Archivos de Empaque", type="primary"):
        csvs = [f for f in [csv_t2, csv_s2, csv_k2] if f is not None]
        
        # BLINDAJE ANTI-DUPLICADOS TAMBIÉN AQUÍ
        raw_emps2 = [e.strip().upper() for e in e_in.split(',') if e.strip()]
        emps2 = list(dict.fromkeys(raw_emps2))
        
        if not csvs: st.error("Sube los CSVs base para armar la matemática.")
        else:
            with st.spinner("Cruzando plataformas con funciones originales..."):
                dicc_nom2, dicc_tipo2 = {}, {}
                if f_base2:
                    try:
                        df_b2 = pd.read_excel(f_base2, sheet_name='BASE')
                    except Exception:
                        df_b2 = pd.read_excel(f_base2)
                    df_b2.columns = df_b2.columns.str.strip().str.upper()
                    for _, r in df_b2.iterrows():
                        s = str(r.get('SKU','')).strip()
                        if s: 
                            dicc_nom2[s] = str(r.get('NOMBRE PLATAFORMA','')).strip()
                            dicc_tipo2[s] = str(r.get('TIPO','NORMAL')).strip().upper()

                dfs2 = [procesar_csv(a, detectar_plataforma_csv(a)[0], detectar_plataforma_csv(a)[1]) for a in csvs]
                df_matriz, _ = unificar_y_distribuir(dfs2, emps2, dicc_nom2, dicc_tipo2, limite_cajas=15)
                
                # --- MAPEO DE JMX PARA TIKTOK ---
                mapa_jmx_to_pedido = {}
                mapa_pedido_to_jmx = {}
                if csv_jmx:
                    plat_jmx, cod_jmx = detectar_plataforma_csv(csv_jmx)
                    if plat_jmx == 'TIKTOK':
                        df_jmx = procesar_csv(csv_jmx, 'TIKTOK', cod_jmx)
                        for _, r in df_jmx.iterrows():
                            ped = str(r.get('PEDIDO','')).replace('.0','').strip()
                            trk = str(r.get('TRACKING_ID','')).replace('.0','').strip()
                            if ped and trk and ped != 'nan' and trk != 'nan': 
                                mapa_jmx_to_pedido[trk] = ped
                                mapa_pedido_to_jmx[ped] = trk
                            
                df_matriz['TRACKING_ID'] = df_matriz.apply(
                    lambda r: mapa_pedido_to_jmx.get(r['PEDIDO'], r.get('TRACKING_ID', '')) if r['PLATAFORMA'] == 'TIKTOK' else r.get('TRACKING_ID', ''), 
                    axis=1
                )

                paginas_por_pedido = {}
                
                if pdf_k2: 
                    reader = PyPDF2.PdfReader(pdf_k2)
                    jmx_actual = None
                    temp_pages = {}
                    for p in reader.pages:
                        m = re.findall(r'(JMX\d+)', p.extract_text() or "")
                        if m: 
                            jmx_actual = str(m[0]).strip()
                            if jmx_actual not in temp_pages: temp_pages[jmx_actual] = []
                        if jmx_actual: 
                            if p not in temp_pages[jmx_actual]: temp_pages[jmx_actual].append(p)
                            
                    for jmx_key, pags in temp_pages.items():
                        pedido_real = mapa_jmx_to_pedido.get(jmx_key, jmx_key)
                        if pedido_real not in paginas_por_pedido: paginas_por_pedido[pedido_real] = []
                        for pag in pags:
                            if pag not in paginas_por_pedido[pedido_real]: paginas_por_pedido[pedido_real].append(pag)

                if pdf_t2: 
                    reader = PyPDF2.PdfReader(pdf_t2)
                    po_actual = None
                    for i, p in enumerate(reader.pages):
                        m = re.findall(r'(PO-\d{3}-\d+)', p.extract_text() or "")
                        if m: 
                            po_actual = str(m[0]).strip()
                            if po_actual not in paginas_por_pedido: 
                                paginas_por_pedido[po_actual] = []
                                if i > 0 and reader.pages[i-1] not in paginas_por_pedido[po_actual]:
                                    paginas_por_pedido[po_actual].append(reader.pages[i-1])
                            if p not in paginas_por_pedido[po_actual]:
                                paginas_por_pedido[po_actual].append(p)
                        else:
                            pass

                if pdf_s2: 
                    reader = PyPDF2.PdfReader(pdf_s2)
                    chunks = []
                    cur = []
                    for p in reader.pages:
                        txt = p.extract_text() or ""
                        if re.search(r'(JMX|GSH|J&T|TODOOR|D2D)', txt.upper()) and 'DECLARACIÓN' not in txt.upper():
                            if cur: chunks.append(cur)
                            cur = [p]
                        else: cur.append(p)
                    if cur: chunks.append(cur)
                    peds_s = df_matriz[df_matriz['PLATAFORMA'] == 'SHEIN'].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique()
                    for i, ped in enumerate(peds_s):
                        if i < len(chunks): paginas_por_pedido[ped] = chunks[i]

                total_encontrados = len(paginas_por_pedido)
                st.metric("🎯 Guías Físicas Encontradas y Cortadas", total_encontrados)

                zip_buf = io.BytesIO()
                colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
                
                with zipfile.ZipFile(zip_buf, "a", zipfile.ZIP_DEFLATED, False) as zf:
                    excel_buf = io.BytesIO()
                    with pd.ExcelWriter(excel_buf, engine='xlsxwriter') as writer:
                        
                        df_asig_ava2 = df_matriz[df_matriz['CATEGORIA'] == 'AVALANCHA'].copy()
                        if not df_asig_ava2.empty:
                            df_asig_ava2 = df_asig_ava2[['ASIGNADO_A', 'PEDIDO', 'TRACKING_ID', 'PLATAFORMA', 'SKU', 'Nombre Correcto']]
                            df_asig_ava2 = df_asig_ava2.sort_values(by=['ASIGNADO_A', 'Nombre Correcto'])
                            df_asig_ava2.rename(columns={'ASIGNADO_A': 'EMPLEADO'}, inplace=True)
                            df_asig_ava2.to_excel(writer, sheet_name='⚡ AVALANCHA GUIAS', index=False)
                        
                        for i, e in enumerate(emps2):
                            df_e = df_matriz[df_matriz['ASIGNADO_A'] == e].copy()
                            df_e = df_e[df_e['PEDIDO'].isin(paginas_por_pedido.keys())]
                            color_actual = colores_division[i % len(colores_division)]
                            
                            if not df_e.empty:
                                df_e[['PEDIDO','TRACKING_ID','PLATAFORMA','SKU','Nombre Correcto','CANTIDAD', 'CATEGORIA']].to_excel(writer, sheet_name=e, index=False)
                                
                                df_ticket = df_e[df_e['CATEGORIA'] == 'CARRITO'].copy()
                                if not df_ticket.empty:
                                    picking_list = df_ticket.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                                    picking_list.rename(columns={'Nombre Correcto': 'Descripción', 'CANTIDAD': 'Total'}, inplace=True)
                                    picking_list = picking_list.sort_values(by='Descripción').reset_index(drop=True)
                                    
                                    hoja_ticket = writer.book.add_worksheet(f"{e}_Ticket")
                                    fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                    fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                    fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                                    fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                                    fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                                    fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                                    
                                    hoja_ticket.write('A1', f'DIVISION {i+1}', fmt_header)
                                    hoja_ticket.write('D1', 'EMPAQUE', fmt_header) 
                                    hoja_ticket.merge_range('A2:D2', e.upper(), fmt_titulo_ticket)
                                    
                                    encabezados = ['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']
                                    for col, encab in enumerate(encabezados):
                                        if encab == 'CANTI\nDAD': hoja_ticket.write(3, col, encab, fmt_wrap)
                                        else: hoja_ticket.write(3, col, encab, fmt_header)
                                        
                                    total_piezas = 0
                                    fila = 4
                                    for idx, item in picking_list.iterrows():
                                        cant = int(item['Total'])
                                        total_piezas += cant
                                        hoja_ticket.write(fila, 0, idx + 1, fmt_td_centro) 
                                        hoja_ticket.write(fila, 1, item['SKU'], fmt_td_centro)            
                                        hoja_ticket.write(fila, 2, item['Descripción'], fmt_td_izq)  
                                        hoja_ticket.write(fila, 3, cant, fmt_td_centro)     
                                        fila += 1
                                        
                                    hoja_ticket.write(fila, 0, len(picking_list) + 1, fmt_td_centro)
                                    hoja_ticket.merge_range(fila, 1, fila, 2, 'Total Empaque', fmt_total)
                                    hoja_ticket.write(fila, 3, total_piezas, fmt_total)
                                    
                                    hoja_ticket.set_column('A:A', 4); hoja_ticket.set_column('B:B', 16); hoja_ticket.set_column('C:C', 38); hoja_ticket.set_column('D:D', 6)
                                    hoja_ticket.set_row(3, 30); hoja_ticket.set_row(1, 25) 
                                    hoja_ticket.fit_to_pages(1, 0); hoja_ticket.set_margins(left=0.1, right=0.1, top=0.1, bottom=0.1)
                                
                                df_ava_pdf = df_e[df_e['CATEGORIA'] == 'AVALANCHA'].copy()
                                if not df_ticket.empty:
                                    p_writer_car = PyPDF2.PdfWriter()
                                    for ped in df_ticket['PEDIDO'].unique():
                                        for pag in paginas_por_pedido[ped]: p_writer_car.add_page(pag)
                                    p_buf_car = io.BytesIO()
                                    p_writer_car.write(p_buf_car)
                                    zf.writestr(f"2_CARRITO_{e}.pdf", p_buf_car.getvalue())
                                    
                                if not df_ava_pdf.empty:
                                    p_writer_ava = PyPDF2.PdfWriter()
                                    for ped in df_ava_pdf['PEDIDO'].unique():
                                        for pag in paginas_por_pedido[ped]: p_writer_ava.add_page(pag)
                                    p_buf_ava = io.BytesIO()
                                    p_writer_ava.write(p_buf_ava)
                                    zf.writestr(f"1_AVALANCHA_{e}.pdf", p_buf_ava.getvalue())
                                
                    zf.writestr(f"Auditoria_Tickets_{datetime.now().strftime('%d-%m-%Y')}.xlsx", excel_buf.getvalue())
                
                st.balloons()
                st.download_button("📦 Descargar ZIP de Empaque Final", zip_buf.getvalue(), f"Empaque_Vmingo_{datetime.now().strftime('%d-%m-%Y')}.zip", "application/zip")
