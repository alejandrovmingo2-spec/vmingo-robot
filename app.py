app.py
import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile

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
                if 'ID del pedido' in linea and 'sku de contribución' in linea:
                    return 'TEMU', cod
                if 'Order ID' in linea and 'Seller SKU' in linea:
                    return 'TIKTOK', cod
        except:
            pass
    return 'DESCONOCIDA', None

st.title("🤖 Robot Multiplataforma Vmingo")
st.markdown("Sube tus documentos para repartir las guías de forma equitativa.")

# --- INTERFAZ DE USUARIO ---
col1, col2 = st.columns(2)
with col1:
    archivo_csv = st.file_uploader("1. Sube el CSV (Temu/TikTok)", type=["csv"])
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
            st.error("❌ ERROR: No pude identificar si el CSV es de Temu o de TikTok. Verifica las columnas.")
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

        # --- LEYENDO PDF ---
        paginas_por_po = {}
        reader = PyPDF2.PdfReader(archivo_pdf)
        
        patron_pdf = r'PO-\d{3}-\d+' if plataforma == 'TEMU' else r'(JMX\d+)' 
        po_actual = None 
        
        for num_pagina, pagina in enumerate(reader.pages):
            texto = pagina.extract_text()
            matches = re.findall(patron_pdf, texto) if texto else []
            
            if matches:
                po_encontrado = str(matches[0]).strip()
                po_actual = po_encontrado 
                
                if po_actual not in paginas_por_po:
                    paginas_por_po[po_actual] = []
                    if plataforma == 'TEMU' and num_pagina > 0:
                        paginas_por_po[po_actual].append(reader.pages[num_pagina - 1])
                
                if pagina not in paginas_por_po[po_actual]:
                    paginas_por_po[po_actual].append(pagina)
            else:
                if plataforma == 'TIKTOK' and po_actual:
                    if pagina not in paginas_por_po[po_actual]:
                        paginas_por_po[po_actual].append(pagina)

        lista_pos_unicos = list(paginas_por_po.keys())
        st.info(f"📄 Se encontraron {len(lista_pos_unicos)} pedidos en el PDF.")

        # --- CRUZANDO CON CSV ---
        texto_csv = archivo_csv.getvalue().decode(codificacion)
        lineas = texto_csv.splitlines()
        skip_lineas = 0
        for i, linea in enumerate(lineas):
            if (plataforma == 'TEMU' and 'ID del pedido' in linea) or (plataforma == 'TIKTOK' and 'Order ID' in linea):
                skip_lineas = i
                break
                
        archivo_csv.seek(0) # Reiniciar lectura
        df = pd.read_csv(archivo_csv, skiprows=skip_lineas, encoding=codificacion)
        df.columns = df.columns.str.strip()

        if plataforma == 'TEMU':
            columnas_utiles = ['ID del pedido', 'sku de contribución', 'nombre del producto', 'variación', 'cantidad a enviar']
            df_filtrado = df[columnas_utiles].copy()
            df_filtrado.rename(columns={
                'ID del pedido': 'PEDIDO', 'sku de contribución': 'SKU',
                'nombre del producto': 'NOMBRE_ORIGINAL', 'variación': 'VARIACION',
                'cantidad a enviar': 'CANTIDAD'
            }, inplace=True)
        else: # TIKTOK
            columnas_utiles = ['Order ID', 'Seller SKU', 'Product Name', 'Variation', 'Quantity', 'Tracking ID']
            columnas_existentes = [col for col in columnas_utiles if col in df.columns]
            df_filtrado = df[columnas_existentes].copy()
            
            if 'Order ID' in df_filtrado.columns and 'Product Name' in df_filtrado.columns and 'Variation' in df_filtrado.columns:
                df_filtrado = df_filtrado.drop_duplicates(subset=['Order ID', 'Product Name', 'Variation'])
            
            df_filtrado.rename(columns={
                'Tracking ID': 'PEDIDO',
                'Seller SKU': 'SKU',
                'Product Name': 'NOMBRE_ORIGINAL', 'Variation': 'VARIACION',
                'Quantity': 'CANTIDAD'
            }, inplace=True)

        df_filtrado = df_filtrado.dropna(subset=['PEDIDO'])
        df_filtrado['PEDIDO'] = df_filtrado['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
        df_filtrado['CANTIDAD'] = pd.to_numeric(df_filtrado['CANTIDAD'], errors='coerce').fillna(0)
        df_filtrado = df_filtrado[df_filtrado['CANTIDAD'] > 0]

        df_filtrado['Nombre Correcto'] = df_filtrado.apply(
            lambda fila: limpiar_nombre(
                diccionario_nombres.get(
                    str(fila['SKU']).strip(),
                    f"{fila.get('NOMBRE_ORIGINAL', '')} - Var: {fila.get('VARIACION', 'N/A')}" 
                )
            ), axis=1
        )

        filas_ordenadas = []
        for po in lista_pos_unicos:
            datos_po = df_filtrado[df_filtrado['PEDIDO'] == po]
            if not datos_po.empty:
                filas_ordenadas.append(datos_po)

        df_ordenado = pd.concat(filas_ordenadas) if filas_ordenadas else pd.DataFrame()

        if df_ordenado.empty:
            st.error("❌ ERROR: Ningún pedido del PDF coincidió con el CSV.")
            st.stop()

        # --- REPARTICIÓN Y CREACIÓN DE ARCHIVOS EN MEMORIA ---
        num_empleados = len(empleados)
        pos_base = len(lista_pos_unicos) // num_empleados
        sobrantes = len(lista_pos_unicos) % num_empleados
        cantidades_por_empleado = [pos_base + (1 if i < sobrantes else 0) for i in range(num_empleados)]

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                indice_inicio = 0
                for i, emp in enumerate(empleados):
                    indice_fin = indice_inicio + cantidades_por_empleado[i]
                    pos_del_empleado = lista_pos_unicos[indice_inicio:indice_fin]
                    indice_inicio = indice_fin

                    df_emp = df_ordenado[df_ordenado['PEDIDO'].isin(pos_del_empleado)].copy()

                    if not df_emp.empty:
                        # 1. Crear Pestañas del Excel
                        worksheet = writer.book.add_worksheet(emp)
                        formato_titulo = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': '#D9D9D9', 'border': 1})
                        worksheet.write(0, 0, f"LISTA DE RECOLECCIÓN PARA: {emp.upper()}", formato_titulo)
                        worksheet.write(1, 0, f"Total de guías asignadas: {len(pos_del_empleado)}")

                        picking_list = df_emp.groupby(['SKU', 'Nombre Correcto'])['CANTIDAD'].sum().reset_index()
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
                        worksheet.set_column('C:C', 18)

                        # Orden de guías
                        inicio_t2 = fin_t1 + 4
                        worksheet.write(inicio_t2 - 2, 0, f"ORDEN EXACTO DE GUÍAS DE {emp.upper()}:", formato_titulo)
                        df_emp['PEDIDO'] = pd.Categorical(df_emp['PEDIDO'], categories=pos_del_empleado, ordered=True)
                        df_emp = df_emp.sort_values('PEDIDO')
                        df_emp_detalle = df_emp[['PEDIDO', 'SKU', 'Nombre Correcto', 'CANTIDAD']].copy()
                        df_emp_detalle.rename(columns={'CANTIDAD': 'Cant.'}, inplace=True)

                        fin_t2 = inicio_t2 + len(df_emp_detalle)
                        df_emp_detalle.to_excel(writer, sheet_name=emp, index=False, header=False, startrow=inicio_t2 + 1, startcol=0)
                        worksheet.add_table(inicio_t2, 0, fin_t2, len(df_emp_detalle.columns) - 1, {
                            'columns': [{'header': col} for col in df_emp_detalle.columns], 'style': 'Table Style Light 11'
                        })
                        worksheet.set_column('D:D', 10)

                        # 2. Crear PDFs en memoria
                        pdf_writer = PyPDF2.PdfWriter()
                        for po in pos_del_empleado:
                            for pagina in paginas_por_po[po]:
                                pdf_writer.add_page(pagina)

                        pdf_buffer = io.BytesIO()
                        pdf_writer.write(pdf_buffer)
                        zip_file.writestr(f"Guias_{emp}.pdf", pdf_buffer.getvalue())

            # Guardar el Excel en el ZIP
            zip_file.writestr("Reparticion_Automatizada.xlsx", excel_buffer.getvalue())

        st.balloons()
        st.success("✨ ¡Todo listo! Se ha generado un archivo ZIP con el Excel de repartición y los PDFs individuales.")
        
        st.download_button(
            label="📦 Descargar Todos los Documentos (ZIP)",
            data=zip_buffer.getvalue(),
            file_name="Documentos_Vmingo.zip",
            mime="application/zip",
            type="primary"
        )