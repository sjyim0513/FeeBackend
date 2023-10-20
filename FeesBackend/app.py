from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

# SQLite 데이터베이스 연결
def connect_to_database(db_name):
    return sqlite3.connect(db_name)

@app.route('/get_fees', methods=['GET'])
def get_total_fee():
    # 요청에서 db_name과 table_name을 얻습니다.
    db_name = request.args.get('db_name')
    table_name = request.args.get('table_name')
    if db_name is None or table_name is None:
        return jsonify({'error': 'Both db_name and table_name are required'}), 400

    # 테이블에서 totalfee0와 totalfee1 값을 가져옵니다.
    conn = connect_to_database(db_name)
    cursor = conn.cursor()
    cursor.execute(f"SELECT totalFee0, totalFee1 FROM {table_name}")
    result = cursor.fetchone()
    conn.close()

    if result:
        totalFee0, totalFee1 = result
        return jsonify({'totalFee0': totalFee0, 'totalFee1': totalFee1})
    else:
        return jsonify({'error': 'Table not found'}), 404

if __name__ == "__main__":
    app.run()
