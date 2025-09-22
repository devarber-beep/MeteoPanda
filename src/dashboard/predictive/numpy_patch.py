"""
Parche para compatibilidad con NumPy 2.0
Soluciona el problema de np.float_ que fue removido en NumPy 2.0
"""

import numpy as np

# Parche para compatibilidad con NumPy 2.0
if not hasattr(np, 'float_'):
    np.float_ = np.float64

if not hasattr(np, 'int_'):
    np.int_ = np.int64

if not hasattr(np, 'complex_'):
    np.complex_ = np.complex128

if not hasattr(np, 'bool_'):
    np.bool_ = np.bool_

# Aplicar el parche automáticamente al importar
def apply_numpy_patch():
    """Aplicar parche de compatibilidad de NumPy"""
    pass  # Ya se aplicó arriba

# Importar este módulo antes que Prophet para aplicar el parche
apply_numpy_patch()
