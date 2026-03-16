from oletools.olevba import VBA_Parser
import pathlib
p = pathlib.Path('lance_requete_par_grappev1.9.xls')
vb = VBA_Parser(str(p))
with open('vba_extracted.txt','w', encoding='utf-8') as f:
    f.write('Has VBA: ' + str(vb.detect_vba_macros()) + '\n')
    for (subfilename, stream_path, vba_filename, vba_code) in vb.extract_macros():
        f.write('--- MODULE: ' + str(vba_filename) + ' ---\n')
        f.write((vba_code or '') + '\n\n')
vb.close()
print('wrote vba_extracted.txt')
