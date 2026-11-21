[Setup]
AppName=Yesbee Catalog
AppVersion=1.0.0
AppPublisher=Yesbee Catalog
DefaultDirName={autopf}\YesbeeCatalog
DisableProgramGroupPage=yes
OutputDir=dist
OutputBaseFilename=YesbeeCatalogSetup
Compression=lzma
SolidCompression=yes

[Files]
Source: "dist\YesbeeCatalog\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autodesktop}\Yesbee Catalog"; Filename: "{app}\YesbeeCatalog.exe"
Name: "{autoprograms}\Yesbee Catalog"; Filename: "{app}\YesbeeCatalog.exe"
