"""
Verification Server - Backend para Script Verifier
===================================================

Servidor Flask que expone el verificador de scripts como API REST.

Endpoints:
- POST /verify - Verifica dos scripts y retorna reporte
- POST /verify-ddv-edv - Verifica conversión DDV→EDV
- GET /health - Health check

Autor: Claude Code
Versión: 1.0
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import tempfile
import os
from script_verifier import verify_scripts, VerificationReport
import traceback

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'service': 'verification-server',
        'version': '1.0.0'
    })


@app.route('/verify', methods=['POST'])
def verify():
    """
    Verifica dos scripts y retorna reporte de diferencias

    Request Body:
    {
        "script1": "contenido del script 1",
        "script2": "contenido del script 2",
        "script1_name": "nombre opcional",
        "script2_name": "nombre opcional",
        "is_ddv_edv": false
    }

    Response:
    {
        "success": true,
        "report": { ... }
    }
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data provided'
            }), 400

        script1_content = data.get('script1')
        script2_content = data.get('script2')
        script1_name = data.get('script1_name', 'script1.py')
        script2_name = data.get('script2_name', 'script2.py')
        is_ddv_edv = data.get('is_ddv_edv', False)

        if not script1_content or not script2_content:
            return jsonify({
                'success': False,
                'error': 'Both script1 and script2 are required'
            }), 400

        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as tmp1:
            tmp1.write(script1_content)
            tmp1_path = tmp1.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as tmp2:
            tmp2.write(script2_content)
            tmp2_path = tmp2.name

        try:
            # Verify scripts
            report = verify_scripts(tmp1_path, tmp2_path, is_ddv_edv)

            # Override names
            report.script1_name = script1_name
            report.script2_name = script2_name

            return jsonify({
                'success': True,
                'report': report.to_dict()
            })

        finally:
            # Clean up temporary files
            try:
                os.unlink(tmp1_path)
                os.unlink(tmp2_path)
            except:
                pass

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/verify-ddv-edv', methods=['POST'])
def verify_ddv_edv():
    """
    Endpoint especializado para verificar conversión DDV→EDV

    Request Body:
    {
        "ddv_script": "contenido script DDV",
        "edv_script": "contenido script EDV"
    }
    """
    try:
        data = request.get_json()

        ddv_script = data.get('ddv_script')
        edv_script = data.get('edv_script')

        if not ddv_script or not edv_script:
            return jsonify({
                'success': False,
                'error': 'Both ddv_script and edv_script are required'
            }), 400

        # Detectar nombre del script
        script_name = 'script'
        if 'MATRIZTRANSACCIONAGENTE' in ddv_script:
            script_name = 'MATRIZTRANSACCIONAGENTE'
        elif 'MATRIZTRANSACCIONCAJERO' in ddv_script:
            script_name = 'MATRIZTRANSACCIONCAJERO'
        elif 'MATRIZTRANSACCIONPOSMACROGIRO' in ddv_script:
            script_name = 'MATRIZTRANSACCIONPOSMACROGIRO'

        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as tmp_ddv:
            tmp_ddv.write(ddv_script)
            tmp_ddv_path = tmp_ddv.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as tmp_edv:
            tmp_edv.write(edv_script)
            tmp_edv_path = tmp_edv.name

        try:
            # Verify with DDV→EDV mode
            report = verify_scripts(tmp_ddv_path, tmp_edv_path, is_ddv_edv=True)

            # Override names
            report.script1_name = f'{script_name}_DDV.py'
            report.script2_name = f'{script_name}_EDV.py'

            return jsonify({
                'success': True,
                'report': report.to_dict()
            })

        finally:
            # Clean up
            try:
                os.unlink(tmp_ddv_path)
                os.unlink(tmp_edv_path)
            except:
                pass

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


if __name__ == '__main__':
    print("="*60)
    print("🚀 Verification Server")
    print("="*60)
    print("📍 Endpoints:")
    print("   • GET  /health")
    print("   • POST /verify")
    print("   • POST /verify-ddv-edv")
    print("="*60)
    print("\n🌐 Starting server on http://localhost:5000")
    print("   Press Ctrl+C to stop\n")

    app.run(host='0.0.0.0', port=5000, debug=True)
