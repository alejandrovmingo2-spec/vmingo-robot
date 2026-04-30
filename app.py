import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Vmingo ERP - Comando Central", page_icon="🤖", layout="wide")

# =====================================================================
# BUSCADOR ROBUSTO DE COLUMNAS Y LECTURA
# =====================================================================
def limpiar_nombre(texto):
    idx = texto.lower().find('detalle')
    if idx != -1: return texto[:idx].strip()
    return texto.strip()

def encontrar_columna(cols_map, palabras_clave):
    for clave, original in cols_map.items():
        if any(palabra in clave for palabra in palabras_clave):
            return original
    return None

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

def procesar_csv(archivo, codificacion, plataforma):
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
    
    df = pd.read_csv(archivo, skiprows=skip_lineas, encoding=codificacion, dtype=str)
    df = df.dropna(how='all')
    cols_map = {c.lower().strip(): c for c in df.columns}
    df_f = pd.DataFrame()

    if plataforma == 'TEMU':
        col_pedido = cols_map.get('id del pedido')
        col_sku = cols_map.get('sku de contribución', cols_map.get('sku de contribucion'))
        col_nom = cols_map.get('nombre del producto')
        col_cant = cols_map.get('cantidad a enviar')
        col_track = encontrar_columna(cols_map, ['seguimiento', 'tracking'])
        
        df_f['ORDER_ID'] = df[col_pedido]
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        df_f['PEDIDO'] = df_f['ORDER_ID'] 
        df_f['SKU'] = df[col_sku]
        df_f['NOMBRE_ORIGINAL'] = df[col_nom]
        df_f['CANTIDAD'] = df[col_cant]
        
    elif plataforma == 'TIKTOK':
        col_order = cols_map.get('order id', cols_map.get('id de pedido'))
        col_track = encontrar_columna(cols_map, ['tracking id', 'seguimiento'])
        col_sku = cols_map.get('seller sku', cols_map.get('sku del vendedor'))
        col_nom = cols_map.get('product name', cols_map.get('nombre del producto'))
        col_cant = cols_map.get('quantity', cols_map.get('cantidad'))
        
        df_f['ORDER_ID'] = df[col_order]
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        df_f['PEDIDO'] = df_f['TRACKING_ID'].replace('', pd.NA).fillna(df_f['ORDER_ID'])
        df_f['SKU'] = df[col_sku]
        df_f['NOMBRE_ORIGINAL'] = df[col_nom]
        df_f['CANTIDAD'] = df[col_cant]
        
    elif plataforma == 'SHEIN':
        col_pedido = cols_map.get('número de pedido', cols_map.get('numero de pedido'))
        col_track = encontrar_columna(cols_map, ['número de guía', 'numero de guia', 'tracking', 'carta de porte'])
        col_sku = cols_map.get('sku del vendedor')
        col_nom = cols_map.get('nombre del producto')
        
        df_f['ORDER_ID'] = df[col_pedido]
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        df_f['PEDIDO'] = df_f['ORDER_ID']
        df_f['SKU'] = df[col_sku]
        df_f['NOMBRE_ORIGINAL'] = df[col_nom]
        df_f['CANTIDAD'] = 1
        
    df_f['PLATAFORMA'] = plataforma
    df_f['ORDEN_ORIGINAL'] = range(len(df_f)) 
    df_f['PEDIDO'] = df_f['PEDIDO'].fillna('').astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
    df_f['ORDER_ID'] = df_f['ORDER_ID'].fillna('').astype(str).str.strip()
    df_f['TRACKING_ID'] = df_f['TRACKING_ID'].fillna('').astype(str).str.strip()
    return df_f[df_f['PEDIDO'] != 'nan']

# =====================================================================
# EL CEREBRO DE REPARTICIÓN 
# =====================================================================
def unificar_y_distribuir(dataframes, empleados, dicc_nombres, dicc_tipos, activar_avalancha=True):
    df_total = pd.concat(dataframes, ignore_index=True)
    df_total['SKU'] = df_total['SKU'].astype(str).str.strip()
    df_total['CANTIDAD'] = pd.to_numeric(df_total['CANTIDAD'], errors='coerce').fillna(1)
    
    df_total['Nombre Correcto'] = df_total['SKU'].apply(lambda x: limpiar_nombre(dicc_nombres.get(x, "SIN NOMBRE EN BASE")))
    df_total['TIPO'] = df_total['SKU'].apply(lambda x: dicc_tipos.get(x, "NORMAL"))
    
    conteo_pedidos = df_total.groupby('PEDIDO')['SKU'].nunique().reset_index()
    conteo_pedidos.columns = ['PEDIDO', 'TIPOS_PRODUCTO']
    df_total = df_total.merge(conteo_pedidos, on='PEDIDO')
    
    if activar_avalancha:
        df_single = df_total[df_total['TIPOS_PRODUCTO'] == 1]
        top_5_skus = df_single.groupby('SKU')['CANTIDAD'].sum().nlargest(5).index.tolist()
        df_total['CATEGORIA'] = df_total.apply(
            lambda r: 'AVALANCHA' if (r['TIPOS_PRODUCTO'] == 1 and r['SKU'] in top_5_skus) else 'CARRITO', axis=1
        )
    else:
        df_total['CATEGORIA'] = 'CARRITO'
    
    asignaciones = {}
    emp_idx = 0
    num_emp = len(empleados)
    
    for plat in ['TIKTOK', 'SHEIN', 'TEMU']:
        df_plat = df_total[df_total['PLATAFORMA'] == plat].copy()
        if df_plat.empty: continue
        
        if activar_avalancha:
            df_ava = df_plat[df_plat['CATEGORIA'] == 'AVALANCHA']
            pedidos_ava = df_ava['PEDIDO'].unique().tolist()
            if plat == 'SHEIN': pedidos_ava = df_plat[df_plat['PEDIDO'].isin(pedidos_ava)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
            else: pedidos_ava.sort()
            for p in pedidos_ava:
                asignaciones[p] = empleados[emp_idx % num_emp]
                emp_idx += 1

        df_car = df_plat[df_plat['CATEGORIA'] == 'CARRITO']
        df_mini_norm = df_car[(df_car['TIPOS_PRODUCTO'] == 1) & (df_car['TIPO'] != 'CAJA')]
        sku_vol_norm = df_mini_norm.groupby('SKU')['CANTIDAD'].sum().sort_values(ascending=False).index.tolist()
        for sku in sku_vol_norm:
            peds_sku = df_mini_norm[df_mini_norm['SKU'] == sku]['PEDIDO'].unique().tolist()
            if plat == 'SHEIN': peds_sku = df_plat[df_plat['PEDIDO'].isin(peds_sku)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
            else: peds_sku.sort()
            emp_asignado = empleados[emp_idx % num_emp] 
            for p in peds_sku: asignaciones[p] = emp_asignado
            emp_idx += 1

        limite_cajas = 15
        df_mini_caja = df_car[(df_car['TIPOS_PRODUCTO'] == 1) & (df_car['TIPO'] == 'CAJA')]
        sku_vol_caja = df_mini_caja.groupby('SKU')['CANTIDAD'].sum().sort_values(ascending=False).index.tolist()
        for sku in sku_vol_caja:
            peds_sku = df_mini_caja[df_mini_caja['SKU'] == sku]['PEDIDO'].unique().tolist()
            if plat == 'SHEIN': peds_sku = df_plat[df_plat['PEDIDO'].isin(peds_sku)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
            else: peds_sku.sort()
            for i in range(0, len(peds_sku), limite_cajas):
                chunk = peds_sku[i : i + limite_cajas]
                emp_asignado = empleados[emp_idx % num_emp]
                for p in chunk: asignaciones[p] = emp_asignado
                emp_idx += 1

        df_mixtos = df_car[df_car['TIPOS_PRODUCTO'] > 1]
        pedidos_mixtos = df_mixtos['PEDIDO'].unique().tolist()
        if plat == 'SHEIN': 
            pedidos_mixtos = df_plat[df_plat['PEDIDO'].isin(pedidos_mixtos)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
        else: 
            cant_mix = df_mixtos.groupby('PEDIDO')['CANTIDAD'].sum().to_dict()
            pedidos_mixtos.sort(key=lambda x: cant_mix.get(x, 0), reverse=True)
        for p in pedidos_mixtos:
            asignaciones[p] = empleados[emp_idx % num_emp]
            emp_idx += 1
                
    df_total['ASIGNADO_A'] = df_total['PEDIDO'].map(asignaciones)
    return df_total

# =====================================================================
# TABS INTERFAZ Y HEADER (LOGO)
# =====================================================================
col_logo, col_title = st.columns([1, 5])
with col_logo:
    try:
        # Asegúrate de nombrar tu imagen exactamente así en tu carpeta
        st.image("logo_vmingo.png", use_column_width=True)
    except:
        pass # Si no encuentra el logo, la app sigue funcionando
with col_title:
    st.title("🤖 Vmingo ERP: Coordinador Maestro")

tab1, tab2 = st.tabs(["🛒 FASE 1: Picking (8 AM)", "📦 FASE 2: Empaque (PDFs)"])

# ----------------- FASE 1 -----------------
with tab1:
    st.markdown("### 1. Planificación Matutina")
    col_t, col_s, col_k = st.columns(3)
    with col_t: f_t = st.file_uploader("CSV TEMU", type=["csv"], key="t1")
    with col_s: f_s = st.file_uploader("CSV SHEIN", type=["csv"], key="s1")
    with col_k: f_k = st.file_uploader("CSV TIKTOK", type=["csv"], key="k1")
    col_b, col_e = st.columns([1, 2])
    with col_b: f_base = st.file_uploader("BASE", type=["xlsx", "xlsm"], key="b1")
    with col_e: e_in = st.text_input("Equipo en Turno:", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR")

    if f_t or f_s or f_k:
        temp_dfs = []
        if f_t: temp_dfs.append(procesar_csv(f_t, detectar_plataforma_csv(f_t)[1], 'TEMU'))
        if f_s: temp_dfs.append(procesar_csv(f_s, detectar_plataforma_csv(f_s)[1], 'SHEIN'))
        if f_k: temp_dfs.append(procesar_csv(f_k, detectar_plataforma_csv(f_k)[1], 'TIKTOK'))
        total_peds = pd.concat(temp_dfs)['PEDIDO'].nunique() if temp_dfs else 0
        
        if total_peds < 600:
            st.warning(f"⚠️ Detectamos {total_peds} pedidos.")
            usar_ava = st.radio("¿Deseas activar AVALANCHA?", ["SÍ", "NO"], index=1)
        else:
            usar_ava = "SÍ"
            st.info(f"📊 {total_peds} pedidos detectados. Avalancha automática.")

        if st.button("📊 Generar Picking", type="primary"):
            raw_emps = [e.strip().upper() for e in e_in.split(',') if e.strip()]
            emps = list(dict.fromkeys(raw_emps)) 
            with st.spinner("Procesando..."):
                dicc_nom, dicc_tipo = {}, {}
                if f_base:
                    try: df_b = pd.read_excel(f_base, sheet_name='BASE', dtype=str)
                    except: df_b = pd.read_excel(f_base, dtype=str)
                    df_b.columns = df_b.columns.str.strip().str.upper()
                    for _, r in df_b.iterrows():
                        s = str(r.get('SKU','')).strip()
                        if s: dicc_nom[s] = str(r.get('NOMBRE PLATAFORMA','')).strip(); dicc_tipo[s] = str(r.get('TIPO','NORMAL')).strip().upper()

                df_final = unificar_y_distribuir(temp_dfs, emps, dicc_nom, dicc_tipo, activar_avalancha=(usar_ava=="SÍ"))
                
                # --- GUARDAR EN MEMORIA ---
                st.session_state['df_matriz_global'] = df_final.copy()
                st.session_state['emps2_global'] = emps
                st.session_state['tot_f2_global'] = total_peds
                st.session_state['usar_ava_global'] = usar_ava
                
                output = io.BytesIO()
                colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
                
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    if usar_ava == "SÍ":
                        df_resumen = df_final[df_final['CATEGORIA'] == 'AVALANCHA'].groupby(['SKU','Nombre Correcto']).agg(CANTIDAD=('CANTIDAD', 'sum'), TIENE_TEMU=('PLATAFORMA', lambda x: 'TEMU' in x.values)).reset_index()
                        df_resumen[['SKU', 'Nombre Correcto', 'CANTIDAD']].sort_values(by='CANTIDAD', ascending=False).to_excel(writer, sheet_name='🔥 TOP 5 AVALANCHA', index=False)
                        df_asig_ava = df_final[df_final['CATEGORIA'] == 'AVALANCHA'][['ASIGNADO_A', 'PLATAFORMA', 'ORDER_ID', 'TRACKING_ID', 'SKU', 'Nombre Correcto', 'CANTIDAD']]
                        df_asig_ava.rename(columns={'ASIGNADO_A': 'EMPLEADO'}, inplace=True)
                        df_asig_ava.to_excel(writer, sheet_name='⚡ ASIGNACION AVALANCHA', index=False)
                    
                    for i, e in enumerate(emps):
                        df_e = df_final[df_final['ASIGNADO_A'] == e].copy()
                        if not df_e.empty:
                            df_e[['PLATAFORMA', 'ORDER_ID', 'TRACKING_ID', 'SKU', 'Nombre Correcto', 'CANTIDAD', 'CATEGORIA']].to_excel(writer, sheet_name=f"{e}_Detalles", index=False)
                            
                            df_tkt = df_e[df_e['CATEGORIA'] == 'CARRITO'].copy()
                            if not df_tkt.empty:
                                picking_list = df_tkt.groupby(['SKU', 'Nombre Correcto']).agg(CANTIDAD=('CANTIDAD', 'sum'), TIENE_TEMU=('PLATAFORMA', lambda x: 'TEMU' in x.values)).reset_index()
                                picking_list = picking_list.sort_values(by='Nombre Correcto').reset_index(drop=True)
                                color_actual = colores_division[i % len(colores_division)]
                                hoja_ticket = writer.book.add_worksheet(f"🛒 {e}_Ticket")
                                fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                                fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                                fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                                fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                                
                                hoja_ticket.write('A1', f'DIVISION {i+1}', fmt_header); hoja_ticket.write('D1', 'CARRITO', fmt_header) 
                                hoja_ticket.merge_range('A2:D2', f"SURTIR: {e.upper()}", fmt_titulo_ticket)
                                for col, enc in enumerate(['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']):
                                    if enc == 'CANTI\nDAD': hoja_ticket.write(3, col, enc, fmt_wrap)
                                    else: hoja_ticket.write(3, col, enc, fmt_header)
                                total_p = 0
                                for row_idx, item in picking_list.iterrows():
                                    cant = int(item['CANTIDAD']); total_p += cant
                                    nom = f"🟢 [TEMU - AVISAR] {item['Nombre Correcto']}" if item['TIENE_TEMU'] else item['Nombre Correcto']
                                    hoja_ticket.write(row_idx + 4, 0, row_idx + 1, fmt_td_centro) 
                                    hoja_ticket.write(row_idx + 4, 1, item['SKU'], fmt_td_centro)            
                                    hoja_ticket.write(row_idx + 4, 2, nom, fmt_td_izq)  
                                    hoja_ticket.write(row_idx + 4, 3, cant, fmt_td_centro)     
                                hoja_ticket.write(len(picking_list) + 4, 0, len(picking_list) + 1, fmt_td_centro)
                                hoja_ticket.merge_range(len(picking_list) + 4, 1, len(picking_list) + 4, 2, 'Total de Carrito', fmt_total)
                                hoja_ticket.write(len(picking_list) + 4, 3, total_p, fmt_total)
                                hoja_ticket.set_column('A:A', 4); hoja_ticket.set_column('B:B', 16); hoja_ticket.set_column('C:C', 38); hoja_ticket.set_column('D:D', 6)
                st.success("✅ Memoria guardada. Picking listo.")
                st.download_button("📥 Descargar Excel Picking", output.getvalue(), f"Picking_{datetime.now().strftime('%d-%m-%Y')}.xlsx", "application/vnd.ms-excel")

# ----------------- FASE 2 -----------------
with tab2:
    st.markdown("### 2. Generador de Guías (Fase 2)")
    
    # --- SISTEMA DE MEMORIA Y ALERTA VISUAL ---
    if 'df_matriz_global' in st.session_state:
        st.success("🟢 🧠 ¡MEMORIA ACTIVA! La distribución de las 8 AM está guardada. Solo sube los PDFs y el JMX.")
        usar_memoria = True
    else:
        st.error("🔴 ⚠️ ¡MEMORIA BORRADA! (Por refresh de la página). Para rescatar guías, SUBE LOS 3 CSVs DE LA MAÑANA AQUÍ ABAJO para reconstruir la matemática exacta.")
        usar_memoria = False
        
    col_t2, col_s2, col_k2 = st.columns(3)
    with col_t2:
        if not usar_memoria: csv_t2 = st.file_uploader("CSV Temu (OBLIGATORIO sin memoria)", type=["csv"], key="ct2")
        pdf_t2 = st.file_uploader("PDF Temu (Súbelo solo si ya lo tienes)", type=["pdf"], key="pt2")
    with col_s2:
        if not usar_memoria: csv_s2 = st.file_uploader("CSV Shein (OBLIGATORIO sin memoria)", type=["csv"], key="cs2")
        pdf_s2 = st.file_uploader("PDF Shein", type=["pdf"], key="ps2")
    with col_k2:
        if not usar_memoria: csv_k2 = st.file_uploader("CSV TikTok (OBLIGATORIO sin memoria)", type=["csv"], key="ck2")
        csv_jmx = st.file_uploader("CSV TikTok JMX", type=["csv"], key="cjmx")
        pdf_k2 = st.file_uploader("PDF TikTok", type=["pdf"], key="pk2")
    
    if not usar_memoria: f_base2 = st.file_uploader("BASE (OBLIGATORIO sin memoria)", type=["xlsx", "xlsm"], key="b2")

    if st.button("✂️ Cortar Guías", type="primary"):
        
        with st.spinner("Ensamblando PDFs y Excel..."):
            if usar_memoria:
                df_matriz = st.session_state['df_matriz_global'].copy()
                emps2 = st.session_state['emps2_global']
                tot_f2 = st.session_state['tot_f2_global']
            else:
                csvs2 = [f for f in [csv_t2, csv_s2, csv_k2] if f is not None]
                if len(csvs2) < 3:
                    st.error("🚨 Para reconstruir la memoria sin fallos, DEBES subir los 3 CSVs (Temu, Shein, TikTok).")
                    st.stop()
                    
                raw_emps2 = [e.strip().upper() for e in e_in.split(',') if e.strip()]
                emps2 = list(dict.fromkeys(raw_emps2))
                dicc_nom2, dicc_tipo2 = {}, {}
                if f_base2:
                    try: df_b2 = pd.read_excel(f_base2, sheet_name='BASE', dtype=str)
                    except: df_b2 = pd.read_excel(f_base2, dtype=str)
                    df_b2.columns = df_b2.columns.str.strip().str.upper()
                    for _, r in df_b2.iterrows():
                        s = str(r.get('SKU','')).strip()
                        if s: dicc_nom2[s] = str(r.get('NOMBRE PLATAFORMA','')).strip(); dicc_tipo2[s] = str(r.get('TIPO','NORMAL')).strip().upper()

                processed_dfs = [procesar_csv(a, detectar_plataforma_csv(a)[1], detectar_plataforma_csv(a)[0]) for a in csvs2]
                tot_f2 = pd.concat(processed_dfs)['PEDIDO'].nunique() if processed_dfs else 0
                df_matriz = unificar_y_distribuir(processed_dfs, emps2, dicc_nom2, dicc_tipo2, activar_avalancha=(tot_f2>=600))
            
            # --- MAPEO DE JMX PARA TIKTOK E INYECCIÓN ---
            mapa_tk = {}
            for _, r in df_matriz[df_matriz['PLATAFORMA'] == 'TIKTOK'].iterrows():
                p, o, t = str(r['PEDIDO']), str(r['ORDER_ID']), str(r['TRACKING_ID'])
                if o: mapa_tk[o] = p
                if t: mapa_tk[t] = p
                
            if csv_jmx:
                df_jmx = procesar_csv(csv_jmx, detectar_plataforma_csv(csv_jmx)[1], 'TIKTOK')
                jmx_map = {}
                for _, r in df_jmx.iterrows():
                    o, t = str(r['ORDER_ID']).strip(), str(r['TRACKING_ID']).strip()
                    if o and t and o != 'nan' and t != 'nan': 
                        mapa_tk[t] = o
                        jmx_map[o] = t
                
                mask = df_matriz['PLATAFORMA'] == 'TIKTOK'
                df_matriz.loc[mask, 'TRACKING_ID'] = df_matriz.loc[mask, 'ORDER_ID'].map(jmx_map).fillna(df_matriz.loc[mask, 'TRACKING_ID'])

            paginas_por_pedido = {}
            stats = {"TEMU": 0, "SHEIN": 0, "TIKTOK": 0}

            # =========================================================
            # TU CÓDIGO INTACTO DE TEMU (JALANDO LA PÁGINA ANTERIOR)
            # =========================================================
            if pdf_t2:
                reader_te = PyPDF2.PdfReader(pdf_t2)
                po_actual_te = None
                patron_pdf_te = r'(PO-\d{3}-\d+)'
                for num_pagina, pagina in enumerate(reader_te.pages):
                    texto = pagina.extract_text() or ""
                    matches = re.findall(patron_pdf_te, texto)
                    if matches:
                        po_encontrado = str(matches[0]).strip()
                        po_actual_te = po_encontrado
                        if po_actual_te not in paginas_por_pedido:
                            paginas_por_pedido[po_actual_te] = []
                            if num_pagina > 0:
                                if reader_te.pages[num_pagina - 1] not in paginas_por_pedido[po_actual_te]:
                                    paginas_por_pedido[po_actual_te].append(reader_te.pages[num_pagina - 1])
                        if pagina not in paginas_por_pedido[po_actual_te]:
                            paginas_por_pedido[po_actual_te].append(pagina)
                    else: pass 
                stats["TEMU"] = len([p for p in df_matriz[df_matriz['PLATAFORMA'] == 'TEMU']['PEDIDO'].unique() if p in paginas_por_pedido])

            # =========================================================
            # TU CÓDIGO INTACTO: TIKTOK
            # =========================================================
            if pdf_k2: 
                reader_tk = PyPDF2.PdfReader(pdf_k2)
                temp_pages = {}
                po_actual_tk = None
                patron_pdf_tk = r'(JMX\d+)'
                for pagina in reader_tk.pages:
                    texto = pagina.extract_text() or ""
                    matches = re.findall(patron_pdf_tk, texto)
                    if matches:
                        po_encontrado = str(matches[0]).strip()
                        po_actual_tk = po_encontrado
                        if po_actual_tk not in temp_pages: temp_pages[po_actual_tk] = []
                        if pagina not in temp_pages[po_actual_tk]: temp_pages[po_actual_tk].append(pagina)
                    else:
                        if po_actual_tk:
                            if pagina not in temp_pages[po_actual_tk]: temp_pages[po_actual_tk].append(pagina)
                for jmx_key, pags in temp_pages.items():
                    order_id = mapa_tk.get(jmx_key, jmx_key)
                    if order_id not in paginas_por_pedido: paginas_por_pedido[order_id] = []
                    for pag in pags:
                        if pag not in paginas_por_pedido[order_id]: paginas_por_pedido[order_id].append(pag)
                stats["TIKTOK"] = len([p for p in df_matriz[df_matriz['PLATAFORMA'] == 'TIKTOK']['PEDIDO'].unique() if p in paginas_por_pedido])

            # =========================================================
            # TU CÓDIGO INTACTO: SHEIN
            # =========================================================
            if pdf_s2: 
                reader_sh = PyPDF2.PdfReader(pdf_s2)
                chunks_pdf_sh = []
                chunk_actual_sh = []
                for pagina in reader_sh.pages:
                    texto = pagina.extract_text() or ""
                    texto_upper = texto.upper()
                    es_declaracion = 'DECLARACIÓN DE CONTENIDO' in texto_upper
                    tiene_indicadores = re.search(r'(JMX|GSH|J&T|TODOOR|D2D)', texto_upper)
                    if tiene_indicadores and not es_declaracion:
                        if chunk_actual_sh: chunks_pdf_sh.append(chunk_actual_sh)
                        chunk_actual_sh = [pagina]
                    else:
                        if chunk_actual_sh: chunk_actual_sh.append(pagina)
                        else: chunk_actual_sh = [pagina]
                if chunk_actual_sh: chunks_pdf_sh.append(chunk_actual_sh)
                    
                pos_finales_shein = df_matriz[df_matriz['PLATAFORMA'] == 'SHEIN'].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique()
                for i, pedido_gsh in enumerate(pos_finales_shein):
                    if i < len(chunks_pdf_sh): paginas_por_pedido[pedido_gsh] = chunks_pdf_sh[i]
                stats["SHEIN"] = len([p for p in pos_finales_shein if p in paginas_por_pedido])

            # --- RENDER DE MÉTRICAS FASE 2 ---
            st.subheader("📊 Diagnóstico de Guías Cortadas (Fase 2)")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("🔴 TEMU Cortados", stats["TEMU"]); c2.metric("🟢 SHEIN Cortados", stats["SHEIN"]); c3.metric("🔵 TIKTOK Cortados", stats["TIKTOK"]); c4.metric("🏆 TOTAL", sum(stats.values()))

            zip_buf = io.BytesIO()
            colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
            
            with zipfile.ZipFile(zip_buf, "a", zipfile.ZIP_DEFLATED, False) as zf:
                excel_buf = io.BytesIO()
                with pd.ExcelWriter(excel_buf, engine='xlsxwriter') as writer:
                    
                    df_matriz[~df_matriz['PEDIDO'].isin(paginas_por_pedido.keys())].to_excel(writer, sheet_name='🚨 FALTAN EN PDF', index=False)
                    
                    if tot_f2 >= 600: 
                        df_matriz[df_matriz['CATEGORIA']=='AVALANCHA'][['ASIGNADO_A','PLATAFORMA','ORDER_ID','TRACKING_ID','SKU','Nombre Correcto']].to_excel(writer, sheet_name='⚡ AVALANCHA GUIAS', index=False)
                    
                    for i, e in enumerate(emps2):
                        df_e_completo = df_matriz[df_matriz['ASIGNADO_A'] == e].copy()
                        
                        if not df_e_completo.empty:
                            df_e_completo[['PLATAFORMA','ORDER_ID','TRACKING_ID','SKU','Nombre Correcto','CANTIDAD', 'CATEGORIA']].to_excel(writer, sheet_name=f"{e}_Detalles", index=False)
                            
                            df_tkt = df_e_completo[df_e_completo['CATEGORIA'] == 'CARRITO'].copy()
                            if not df_tkt.empty:
                                picking_list = df_tkt.groupby(['SKU', 'Nombre Correcto']).agg(CANTIDAD=('CANTIDAD', 'sum'), TIENE_TEMU=('PLATAFORMA', lambda x: 'TEMU' in x.values)).reset_index()
                                picking_list = picking_list.sort_values(by='Nombre Correcto').reset_index(drop=True)
                                color_actual = colores_division[i % len(colores_division)]
                                hoja_ticket = writer.book.add_worksheet(f"🛒 {e}_Ticket")
                                fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                                fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                                fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                                fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                                fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                                
                                hoja_ticket.write('A1', f'DIVISION {i+1}', fmt_header); hoja_ticket.write('D1', 'EMPAQUE', fmt_header) 
                                hoja_ticket.merge_range('A2:D2', f"EMPAQUE: {e.upper()}", fmt_titulo_ticket)
                                for col, enc in enumerate(['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']):
                                    if enc == 'CANTI\nDAD': hoja_ticket.write(3, col, enc, fmt_wrap)
                                    else: hoja_ticket.write(3, col, enc, fmt_header)
                                total_p = 0
                                for row_idx, item in picking_list.iterrows():
                                    cant = int(item['CANTIDAD']); total_p += cant
                                    nom = f"🟢 [TEMU - AVISAR] {item['Nombre Correcto']}" if item['TIENE_TEMU'] else item['Nombre Correcto']
                                    hoja_ticket.write(row_idx + 4, 0, row_idx + 1, fmt_td_centro) 
                                    hoja_ticket.write(row_idx + 4, 1, item['SKU'], fmt_td_centro)            
                                    hoja_ticket.write(row_idx + 4, 2, nom, fmt_td_izq)  
                                    hoja_ticket.write(row_idx + 4, 3, cant, fmt_td_centro)     
                                hoja_ticket.write(len(picking_list) + 4, 0, len(picking_list) + 1, fmt_td_centro)
                                hoja_ticket.merge_range(len(picking_list) + 4, 1, len(picking_list) + 4, 2, 'Total de Empaque', fmt_total)
                                hoja_ticket.write(len(picking_list) + 4, 3, total_p, fmt_total)
                                hoja_ticket.set_column('A:A', 4); hoja_ticket.set_column('B:B', 16); hoja_ticket.set_column('C:C', 38); hoja_ticket.set_column('D:D', 6)
                            
                            df_ava_pdf = df_e_completo[df_e_completo['CATEGORIA'] == 'AVALANCHA'].copy()
                            if not df_tkt.empty:
                                pw = PyPDF2.PdfWriter()
                                for p_id in df_tkt['PEDIDO'].unique():
                                    if p_id in paginas_por_pedido:
                                        for pag in paginas_por_pedido[p_id]: pw.add_page(pag)
                                if len(pw.pages) > 0:
                                    buf = io.BytesIO(); pw.write(buf); zf.writestr(f"2_CARRITO_{e}.pdf", buf.getvalue())
                            
                            if not df_ava_pdf.empty:
                                pw = PyPDF2.PdfWriter()
                                for p_id in df_ava_pdf['PEDIDO'].unique():
                                    if p_id in paginas_por_pedido:
                                        for pag in paginas_por_pedido[p_id]: pw.add_page(pag)
                                if len(pw.pages) > 0:
                                    buf = io.BytesIO(); pw.write(buf); zf.writestr(f"1_AVALANCHA_{e}.pdf", buf.getvalue())
                                    
                zf.writestr(f"Auditoria_Empaque.xlsx", excel_buf.getvalue())
            st.download_button("📦 Descargar ZIP Guías", zip_buf.getvalue(), f"Empaque_Vmingo_{datetime.now().strftime('%d-%m-%Y')}.zip", "application/zip")
