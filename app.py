import streamlit as st
import pandas as pd
import io

# ========== ตั้งค่าหน้าเว็บ ========== #
st.set_page_config(page_title="Data Processing App", layout="wide")
st.title("⚙️ Data Processing Automation")
st.markdown("อัปโหลดไฟล์ > เลือกแท็บ > กดปุ่มประมวลผล > ดาวน์โหลดผลลัพธ์")

# ========== ฟังก์ชันการทำงานของโค้ดชุดที่ 1 ========== #
def process_layout_joiner(layout_file, stylelist_file):
    """
    ทำการ Join ไฟล์ layout_master และ stylelistcode
    """
    try:
        layout_master = pd.read_csv(layout_file)
        stylelistcode = pd.read_csv(stylelist_file)

        # ทำการ JOIN ตาม Logic เดิม
        merged_df = pd.merge(
            layout_master, 
            stylelistcode, 
            how='inner', 
            left_on='LINELAYOUT', 
            right_on='LINELAYOUT'
        )
        return merged_df
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดใน Layout Joiner: {e}")
        return None

# ========== ฟังก์ชันการทำงานของโค้ดชุดที่ 2 ========== #
def process_rawdata_model(rawdata_file, stylelist_file):
    """
    ประมวลผล Rawdata เพื่อสร้าง Model
    """
    try:
        rawdata_df = pd.read_csv(rawdata_file)
        stylelistcode_df = pd.read_csv(stylelist_file)

        # 2. ทำความสะอาดชื่อคอลัมน์
        rawdata_df.columns = rawdata_df.columns.str.strip().str.lower()
        stylelistcode_df.columns = stylelistcode_df.columns.str.strip().str.lower()

        # 3. INNER JOIN
        merged_df = pd.merge(rawdata_df, stylelistcode_df, on='group', how='inner')

        # 4. เตรียมคอลัมน์สำคัญ
        required_columns = ['line', 'linkeff', 'linkop', 'id', 'shift', 'style', 'group', 'jobtitle', 'eff']
        for col in required_columns:
            if col not in merged_df.columns:
                merged_df[col] = 0 if pd.api.types.is_numeric_dtype(merged_df[col].dtype) else 'N/A'

        # 5. คำนวณ eff_adjusted
        merged_df['eff'] = pd.to_numeric(merged_df['eff'], errors='coerce').fillna(0)
        merged_df['eff_adjusted'] = merged_df['eff'] * 1.05

        # 6. สร้าง rank
        merged_df['rank'] = merged_df.groupby(['id', 'group', 'jobtitle'])['eff_adjusted'] \
                                     .rank(method='first', ascending=False)

        # 7. กรองข้อมูล
        top3_df = merged_df[(merged_df['rank'] <= 3) & (merged_df['eff'] >= 35)]

        # 8. Group by และหาค่าเฉลี่ย
        agg_df = top3_df.groupby(['linkeff', 'linkop', 'id', 'line', 'shift', 'style', 'group', 'jobtitle'], as_index=False)['eff'].mean()

        # 9. เปลี่ยนชื่อคอลัมน์
        agg_df = agg_df.rename(columns={'eff': 'AvgEff'})
        
        return agg_df
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดใน Raw Data Processor: {e}")
        return None

# ========== ส่วนของ UI (User Interface) ========== #

# --- ส่วนอัปโหลดไฟล์ (Sidebar) --- #
with st.sidebar:
    st.header("📂 อัปโหลดไฟล์ CSV")
    
    uploaded_layout_master = st.file_uploader("1. อัปโหลด layout_master.csv", type=["csv"])
    uploaded_stylelistcode = st.file_uploader("2. อัปโหลด stylelistcode.csv", type=["csv"])
    uploaded_rawdata_all = st.file_uploader("3. อัปโหลด RawdataALL.csv", type=["csv"])

# --- สร้างแท็บสำหรับแต่ละ Process --- #
tab1, tab2 = st.tabs(["Process 1: Layout Joiner", "Process 2: Raw Data Model"])

# --- การทำงานในแท็บที่ 1 --- #
with tab1:
    st.header("🔗 รวมไฟล์ Layout Master และ Style List")
    st.markdown("ใช้ไฟล์ `layout_master.csv` และ `stylelistcode.csv`")
    
    run_process1 = st.button("🚀 เริ่มประมวลผล Layout Joiner", key="btn1")

    if run_process1:
        if uploaded_layout_master and uploaded_stylelistcode:
            with st.spinner('กำลังรวมไฟล์...'):
                # เรียกใช้ฟังก์ชันและเก็บผลลัพธ์ใน session_state
                st.session_state.df_result1 = process_layout_joiner(uploaded_layout_master, uploaded_stylelistcode)
            
            if st.session_state.df_result1 is not None:
                st.success("✅ รวมไฟล์เรียบร้อยแล้ว!")
        else:
            st.warning("⚠️ กรุณาอัปโหลดไฟล์ `layout_master.csv` และ `stylelistcode.csv` ก่อน")

    # แสดงผลลัพธ์และปุ่มดาวน์โหลด (จะแสดงเมื่อมีข้อมูลใน session_state)
    if 'df_result1' in st.session_state and st.session_state.df_result1 is not None:
        st.subheader("📊 ผลลัพธ์:")
        st.dataframe(st.session_state.df_result1)
        
        # แปลง DataFrame เป็น CSV สำหรับดาวน์โหลด
        csv_data = st.session_state.df_result1.to_csv(index=False).encode('utf-8')
        
        st.download_button(
            label="📥 ดาวน์โหลดผลลัพธ์ (Layout_week18_22.csv)",
            data=csv_data,
            file_name='Layout_week18_22.csv',
            mime='text/csv',
        )

# --- การทำงานในแท็บที่ 2 --- #
with tab2:
    st.header("📈 ประมวลผล Raw Data เพื่อสร้างโมเดล")
    st.markdown("ใช้ไฟล์ `RawdataALL.csv` และ `stylelistcode.csv`")

    run_process2 = st.button("🚀 เริ่มประมวลผล Raw Data Model", key="btn2")

    if run_process2:
        if uploaded_rawdata_all and uploaded_stylelistcode:
            with st.spinner('กำลังประมวลผลข้อมูล...'):
                # เรียกใช้ฟังก์ชันและเก็บผลลัพธ์ใน session_state
                st.session_state.df_result2 = process_rawdata_model(uploaded_rawdata_all, uploaded_stylelistcode)
            
            if st.session_state.df_result2 is not None:
                st.success("✅ ประมวลผลข้อมูลเรียบร้อยแล้ว!")
        else:
            st.warning("⚠️ กรุณาอัปโหลดไฟล์ `RawdataALL.csv` และ `stylelistcode.csv` ก่อน")
            
    # แสดงผลลัพธ์และปุ่มดาวน์โหลด
    if 'df_result2' in st.session_state and st.session_state.df_result2 is not None:
        st.subheader("📊 ผลลัพธ์:")
        st.dataframe(st.session_state.df_result2)

        # แปลง DataFrame เป็น CSV สำหรับดาวน์โหลด
        csv_data = st.session_state.df_result2.to_csv(index=False).encode('utf-8')
        
        st.download_button(
            label="📥 ดาวน์โหลดผลลัพธ์ (RAWDATA_MODEL_ALL1.csv)",
            data=csv_data,
            file_name='RAWDATA_MODEL_ALL1.csv',
            mime='text/csv',
        )