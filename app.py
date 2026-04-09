import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Robot Multiplataforma Vmingo", page_icon="🤖", layout="centered")

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
                if 'order id' in lin_low and 'seller sku' in lin_low:
                    return 'TIKTOK', cod
                if 'número de pedido' in lin_low and 'sku del vendedor' in lin_low:
                    return 'SHEIN', cod
        except:
            pass
    return 'DESCONOCIDA', None

st.title("🤖 Robot Multiplataforma Vmingo")
st.markdown("Sube tus documentos para repartir las guías de forma equitativa.")

# --- INTERFAZ DE USUARIO ---
col1, col2 = st.columns(2)
with col1:
    archivo_csv = st.file_uploader("1. Sube el CSV (Temu / TikTok / Shein)", type=["csv"])
    archivo_pdf = st.file_uploader("2. Sube el PDF gigante", type=["pdf"])
with col2:
    archivo_base = st.file_uploader("3. Sube tu BASE (Opcional)", type=["xlsx", "xlsm"])

empleados_input = st.text_input("Nombres del equipo en turno (separados por coma):", "ALDO, BELEN, ALDRICH")

if st.button("🚀 Procesar Guías", type="primary"):
    if not archivo_csv or not archivo_pdf:
        st.error("❌ Error: Faltan archivos. Asegúrate de subir el CSV y el PDF.")
        st.stop()

    empleados = [e.strip() for e in empleados_input.split(',') if e.strip()]
    if not empleados:
        st.error("❌ Error: Necesitas ingresar al menos un nombre en el equipo.")
        st.stop()

    with st.spinner("Analizando documentos..."):
        plataforma, codificacion = detectar_plataforma_web(archivo_csv)
        if plataforma == 'DESCONOCIDA':
            st.error("❌ ERROR: No pude identificar si el CSV es de Temu, TikTok o Shein. Verifica las columnas.")
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
            except Exception as e:
                st.warning(f"⚠️ Hubo un detalle al leer tu hoja BASE: {e}")

        # --- 1. LEYENDO CSV PRIMERO PARA CREAR EL MAPA MAESTRO ---
        texto_csv = archivo_csv.getvalue().decode(codificacion)
        lineas = texto_csv.splitlines()
        skip_lineas = 0
        for i, linea in enumerate(lineas):
            lin_low = linea.lower()
            if (plataforma == 'TEMU' and 'id del pedido' in lin_low) or \
               (plataforma == 'TIKTOK' and 'order id' in lin_low) or \
               (plataforma == 'SHEIN' and 'número de pedido' in lin_low):
                skip_lineas = i
                break
                
        archivo_csv.seek(0) 
        df = pd.read_csv(archivo_csv, skiprows=skip_lineas, encoding=codificacion)
        
        # Mapeo robusto de columnas sin importar mayúsculas
        cols_map = {c.lower().strip(): c for c in df.columns}

        # Diccionario infalible (Une JMX/Tracking con el Pedido Oficial)
        mapa_pedidos = {}
        if plataforma == 'SHEIN':
            col_gsh = cols_map.get('número de pedido', cols_map.get('numero de pedido'))
            col_jmx = cols_map.get('número de guía', cols_map.get('numero de guia'))
            for idx, row in df.iterrows():
                gsh = str(row.get(col_gsh, '')).replace('.0', '').strip() if col_gsh else ''
                jmx = str(row.get(col_jmx, '')).replace('.0', '').strip() if col_jmx else ''
                if gsh and gsh != 'nan':
                    mapa_pedidos[gsh] = gsh
                if jmx and jmx != 'nan':
                    mapa_pedidos[jmx] = gsh
                    
        elif plataforma == 'TIKTOK':
            col_order = cols_map.get('order id')
            col_track = cols_map.get('tracking id')
            for idx, row in df.iterrows():
                order_id = str(row.get(col_order, '')).replace('.0', '').strip() if col_order else ''
                tracking_id = str(row.get(col_track, '')).replace('.0', '').strip() if col_track else ''
                if order_id and order_id != 'nan':
                    mapa_pedidos[order_id] = order_id
                if tracking_id and tracking_id != 'nan':
                    mapa_pedidos[tracking_id] = order_id

        # Limpieza y filtrado del CSV Inteligente
        if plataforma == 'TEMU':
            col_pedido = cols_map.get('id del pedido')
            col_sku = cols_map.get('sku de contribución')
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
            
        elif plataforma == 'TIKTOK':
            col_order = cols_map.get('order id')
            col_sku = cols_map.get('seller sku')
            col_nombre = cols_map.get('product name')
            col_var = cols_map.get('variation')
            col_cant = cols_map.get('quantity')
            
            columnas_utiles = [c for c in [col_order, col_sku, col_nombre, col_var, col_cant] if c]
            df_filtrado = df[columnas_utiles].copy()
            
            if col_order: df_filtrado['PEDIDO'] = df_filtrado[col_order].astype(str).str.strip()
            
            rename_dict = {}
            if col_sku: rename_dict[col_sku] = 'SKU'
            if col_nombre: rename_dict[col_nombre] = 'NOMBRE_ORIGINAL'
            if col_var: rename_dict[col_var] = 'VARIACION'
            if col_cant: rename_dict[col_cant] = 'CANTIDAD'
            df_filtrado.rename(columns=rename_dict, inplace=True)
            
        elif plataforma == 'SHEIN':
            col_pedido = cols_map.get('número de pedido', cols_map.get('numero de pedido'))
            col_sku = cols_map.get('sku del vendedor')
            col_nombre = cols_map.get('nombre del producto')
            col_var = cols_map.get('especificación', cols_map.get('especificacion'))
            col_jmx = cols_map.get('número de guía', cols_map.get('numero de guia')) # RESTAURADO PARA SHEIN
            
            columnas_utiles = [c for c in [col_pedido, col_sku, col_nombre, col_var, col_jmx] if c]
            df_filtrado = df[columnas_utiles].copy()
            
            df_filtrado['CANTIDAD'] = 1
            if col_pedido: df_filtrado['PEDIDO'] = df_filtrado[col_pedido].astype(str).str.strip()
            
            # RESTAURADO: Visión Doble para Shein
            df_filtrado['GUIA'] = df_filtrado.get(col_jmx, pd.Series(dtype=str)).fillna('').astype(str).str.strip() if col_jmx else ''
            
            rename_dict = {}
            if col_sku: rename_dict[col_sku] = 'SKU'
            if col_nombre: rename_dict[col_nombre] = 'NOMBRE_ORIGINAL'
            if col_var: rename_dict[col_var] = 'VARIACION'
            df_filtrado.rename(columns=rename_dict, inplace=True)

        # Escudos Anti-Crash
        if 'PEDIDO' not in df_filtrado.columns:
            st.error("❌ ERROR: No se encontró la columna de Número de Pedido en el CSV.")
            st.stop()
            
        df_filtrado = df_filtrado.dropna(subset=['PEDIDO'])
        df_filtrado['PEDIDO'] = df_filtrado['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
        df_filtrado['PEDIDO_DISPLAY'] = df_filtrado['PEDIDO']
        
        df_filtrado['SKU'] = df_filtrado.get('SKU', pd.Series(dtype=str)).fillna('SIN SKU').astype(str)
        df_filtrado['CANTIDAD'] = pd.to_numeric(df_filtrado.get('CANTIDAD', pd.Series(dtype=int)), errors='coerce').fillna(0)
        df_filtrado = df_filtrado[df_filtrado['CANTIDAD'] > 0]

        df_filtrado['Nombre Correcto'] = df_filtrado.apply(
            lambda fila: limpiar_nombre(
                diccionario_nombres.get(
                    str(fila.get('SKU', '')).strip(),
                    f"{fila.get('NOMBRE_ORIGINAL', '')} - Var: {fila.get('VARIACION', 'N/A')}" 
                )
            ), axis=1
        )
        df_filtrado['Nombre Correcto'] = df_filtrado['Nombre Correcto'].fillna('SIN NOMBRE').astype(str)

        # Extraemos las órdenes únicas directamente del CSV
        pos_finales_reales = list(dict.fromkeys(df_filtrado['PEDIDO'].tolist()))

        # --- 2. LEYENDO PDF ---
        paginas_por_po = {}
        reader = PyPDF2.PdfReader(archivo_pdf)
        
        if plataforma == 'SHEIN':
            chunks_pdf = []
            chunk_actual = []

            for num_pagina, pagina in enumerate(reader.pages):
                texto = pagina.extract_text() or ""
                texto_upper = texto.upper()
                es_declaracion = 'DECLARACIÓN DE CONTENIDO' in texto_upper
                tiene_indicadores = re.search(r'(JMX|GSH|J&T|TODOOR|D2D)', texto_upper)

                if tiene_indicadores and not es_declaracion:
                    if chunk_actual:
                        chunks_pdf.append(chunk_actual)
                    chunk_actual = [pagina]
                else:
                    if chunk_actual:
                        chunk_actual.append(pagina)
                    else:
                        chunk_actual = [pagina]
            if chunk_actual:
                chunks_pdf.append(chunk_actual)

            for bloque in chunks_pdf:
                llave_bloque = None
                for pag in bloque:
                    texto_pag = pag.extract_text() or ""
                    matches = re.findall(r'(JMX\d+|GSH\w+)', texto_pag.upper())
                    if matches:
                        gsh_matches = [m for m in matches if m.startswith('GSH')]
                        if gsh_matches:
                            llave_bloque = gsh_matches[0].strip()
                            break 
                        elif not llave_bloque:
                            llave_bloque = matches[0].strip() 
                
                if llave_bloque:
                    official_gsh = mapa_pedidos.get(llave_bloque, llave_bloque)
                    if official_gsh not in paginas_por_po:
                        paginas_por_po[official_gsh] = []
                    paginas_por_po[official_gsh].extend(bloque)
                    
        else:
            # LÓGICA ORIGINAL RESTAURADA EXACTAMENTE PARA TEMU Y TIKTOK
            patron_pdf = r'(PO-\d{3}-\d+)' if plataforma == 'TEMU' else r'(JMX\d+)'
            po_actual = None 
            
            for num_pagina, pagina in enumerate(reader.pages):
                texto = pagina.extract_text() or ""
                matches = re.findall(patron_pdf, texto)
                
                if matches:
                    po_encontrado = matches[0].strip()
                    po_actual = po_encontrado 
                    
                    if po_actual not in paginas_por_po:
                        paginas_por_po[po_actual] = []
                        if plataforma == 'TEMU' and num_pagina > 0:
                            if reader.pages[num_pagina - 1] not in paginas_por_po[po_actual]:
                                paginas_por_po[po_actual].append(reader.pages[num_pagina - 1])
                    
                    if pagina not in paginas_por_po[po_actual]:
                        paginas_por_po[po_actual].append(pagina)
                else:
                    if plataforma == 'TIKTOK' and po_actual:
                        if pagina not in paginas_por_po[po_actual]:
                            paginas_por_po[po_actual].append(pagina)

            if plataforma == 'TIKTOK':
                paginas_corregidas = {}
                for po_key, paginas in paginas_por_po.items():
                    llave_final = mapa_pedidos.get(po_key, po_key)
                    if llave_final not in paginas_corregidas:
                        paginas_corregidas[llave_final] = []
                    for pag in paginas:
                        if pag not in paginas_corregidas[llave_final]:
                            paginas_corregidas[llave_final].append(pag)
                paginas_por_po = paginas_corregidas
            
        # Filtramos la lista maestra para empacar SOLO lo que se encontró físicamente en el PDF
        lista_pos_pdf = list(paginas_por_po.keys())
        pos_finales_reales = [po for po in pos_finales_reales if po in lista_pos_pdf]

        # --- 3. PREPARANDO DATA FINAL Y EXCEL ---
        filas_ordenadas = []
        indices_agregados = set() # RESTAURADO: Evita duplicar el mismo pedido al cruzar
        
        for po in pos_finales_reales:
            if plataforma == 'SHEIN':
                # RESTAURADO: La Visión Doble para Shein (Busca por GSH o por JMX)
                mask = (df_filtrado['PEDIDO'] == po) | ((df_filtrado['GUIA'] == po) & (df_filtrado['GUIA'] != '') & (df_filtrado['GUIA'] != 'nan'))
                datos_po = df_filtrado[mask].copy()
            else:
                datos_po = df_filtrado[df_filtrado['PEDIDO'] == po].copy()
                
            if not datos_po.empty:
                datos_po = datos_po[~datos_po.index.isin(indices_agregados)]
                if not datos_po.empty:
                    # Estandarizamos para que el PDF y el CSV se llamen igual
                    oficial_pedido = datos_po.iloc[0]['PEDIDO']
                    datos_po['PEDIDO'] = oficial_pedido
                    datos_po['PEDIDO_DISPLAY'] = oficial_pedido
                    
                    filas_ordenadas.append(datos_po)
                    indices_agregados.update(datos_po.index)

        df_ordenado = pd.concat(filas_ordenadas) if filas_ordenadas else pd.DataFrame()

        if df_ordenado.empty:
            st.error("❌ ERROR: Ningún pedido físico en el PDF coincidió con tu Excel.")
            st.stop()

        st.success(f"📄 ÉXITO: Se procesarán y empacarán {len(pos_finales_reales)} pedidos únicos perfectamente sincronizados.")

        df_ordenado['PEDIDO'] = pd.Categorical(df_ordenado['PEDIDO'], categories=pos_finales_reales, ordered=True)
        df_ordenado = df_ordenado.sort_values('PEDIDO')

        # --- REPARTICIÓN Y CREACIÓN DE ARCHIVOS EN MEMORIA ---
        num_empleados = len(empleados)
        pos_base = len(pos_finales_reales) // num_empleados
        sobrantes = len(pos_finales_reales) % num_empleados
        cantidades_por_empleado = [pos_base + (1 if i < sobrantes else 0) for i in range(num_empleados)]

        colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                indice_inicio = 0
                for i, emp in enumerate(empleados):
                    indice_fin = indice_inicio + cantidades_por_empleado[i]
                    pos_del_empleado = pos_finales_reales[indice_inicio:indice_fin]
                    indice_inicio = indice_fin
                    
                    df_emp = df_ordenado[df_ordenado['PEDIDO'].isin(pos_del_empleado)].copy()
                    
                    if not df_emp.empty:
                        color_actual = colores_division[i % len(colores_division)]
                        
                        # 1. Crear Pestañas Principales del Excel
                        worksheet = writer.book.add_worksheet(emp)
                        formato_titulo = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': color_actual, 'border': 1})
                        worksheet.write(0, 0, f"LISTA DE RECOLECCIÓN PARA: {emp.upper()}", formato_titulo)
                        worksheet.write(1, 0, f"Total de guías asignadas: {len(pos_del_empleado)}")
                        
                        # --- TABLA SUPERIOR (Agrupada) ---
                        picking_list = df_emp.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                        picking_list.rename(columns={'Nombre Correcto': 'Descripción (Según BASE)', 'CANTIDAD': 'Total a Recolectar'}, inplace=True)
                        picking_list = picking_list.sort_values(by='Descripción (Según BASE)').reset_index(drop=True)

                        inicio_t1 = 3
                        fin_t1 = inicio_t1 + len(picking_list)
                        picking_list.to_excel(writer, sheet_name=emp, index=False, header=False, startrow=inicio_t1 + 1, startcol=0)
                        worksheet.add_table(inicio_t1, 0, fin_t1, len(picking_list.columns) - 1, {
                            'columns': [{'header': col} for col in picking_list.columns], 'style': 'Table Style Medium 9'
                        })
                        worksheet.set_column('A:A', 20)
                        worksheet.set_column('B:B', 65)

                        # --- TABLA INFERIOR (Agrupada para evitar visuales duplicados) ---
                        fila_orden = fin_t1 + 3
                        worksheet.write(fila_orden, 0, f"ORDEN EXACTO DE GUÍAS DE {emp.upper()}:", formato_titulo)
                        
                        df_orden_imp = df_emp.groupby(['PEDIDO_DISPLAY', 'SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                        df_orden_imp.rename(columns={'PEDIDO_DISPLAY': 'PEDIDO', 'CANTIDAD': 'Cant.'}, inplace=True)
                        df_orden_imp.to_excel(writer, sheet_name=emp, index=False, startrow=fila_orden + 2, startcol=0)
                        
                        # ---------------------------------------------------------
                        # 2. CREAR LA HOJA DE TICKETS (Formato para Térmica)
                        # ---------------------------------------------------------
                        hoja_ticket = writer.book.add_worksheet(f"{emp}_Ticket")
                        
                        fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                        fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                        fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                        fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                        fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                        fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                        
                        num_division = i + 1
                        hoja_ticket.write('A1', f'DIVISION {num_division}', fmt_header)
                        hoja_ticket.write('D1', plataforma.upper(), fmt_header) 
                        hoja_ticket.merge_range('A2:D2', emp.upper(), fmt_titulo_ticket)
                        
                        encabezados = ['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']
                        for col, encabezado in enumerate(encabezados):
                            if encabezado == 'CANTI\nDAD':
                                hoja_ticket.write(3, col, encabezado, fmt_wrap)
                            else:
                                hoja_ticket.write(3, col, encabezado, fmt_header)
                            
                        total_piezas = 0
                        fila = 4
                        
                        for idx, item in picking_list.iterrows():
                            cant = int(item['Total a Recolectar'])
                            total_piezas += cant
                            
                            hoja_ticket.write(fila, 0, idx + 1, fmt_td_centro) 
                            hoja_ticket.write(fila, 1, item['SKU'], fmt_td_centro)            
                            hoja_ticket.write(fila, 2, item['Descripción (Según BASE)'], fmt_td_izq)  
                            hoja_ticket.write(fila, 3, cant, fmt_td_centro)     
                            fila += 1
                            
                        hoja_ticket.write(fila, 0, len(picking_list) + 1, fmt_td_centro)
                        hoja_ticket.merge_range(fila, 1, fila, 2, 'Total general', fmt_total)
                        hoja_ticket.write(fila, 3, total_piezas, fmt_total)
                        
                        hoja_ticket.set_column('A:A', 4)
                        hoja_ticket.set_column('B:B', 16)
                        hoja_ticket.set_column('C:C', 38)
                        hoja_ticket.set_column('D:D', 6)
                        hoja_ticket.set_row(3, 30) 
                        hoja_ticket.set_row(1, 25) 
                        
                        hoja_ticket.fit_to_pages(1, 0) 
                        hoja_ticket.set_margins(left=0.1, right=0.1, top=0.1, bottom=0.1) 
                        
                        # ---------------------------------------------------------
                        # 3. CREAR PDFs EN MEMORIA
                        # ---------------------------------------------------------
                        pdf_writer = PyPDF2.PdfWriter()
                        for po in pos_del_empleado:
                            if po in paginas_por_po:
                                for pagina in paginas_por_po[po]:
                                    pdf_writer.add_page(pagina)

                        pdf_buffer = io.BytesIO()
                        pdf_writer.write(pdf_buffer)
                        zip_file.writestr(f"Guias_{emp}.pdf", pdf_buffer.getvalue())

            zip_file.writestr("Reparticion_Automatizada.xlsx", excel_buffer.getvalue())

        st.session_state['descarga_lista'] = zip_buffer.getvalue()
        st.session_state['plataforma_procesada'] = plataforma

if 'descarga_lista' in st.session_state:
    st.balloons()
    
    plat = st.session_state.get('plataforma_procesada', 'Vmingo')
    fecha = datetime.now().strftime("%d-%m-%Y")
    nombre_archivo = f"Guias_{plat}_{fecha}.zip"
    
    st.success(f"✨ ¡Todo listo! Se ha generado el archivo {nombre_archivo} con el Excel de repartición y los PDFs individuales.")
    
    st.download_button(
        label=f"📦 Descargar {nombre_archivo}",
        data=st.session_state['descarga_lista'],
        file_name=nombre_archivo,
        mime="application/zip",
        type="primary"
    )
