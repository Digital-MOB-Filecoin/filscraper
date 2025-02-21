CREATE TABLE IF NOT EXISTS fil_re_share_electricity_capacity_2023
(
    country TEXT NOT NULL,
    value NUMERIC NOT NULL,
    UNIQUE (country)
    );

CREATE INDEX IF NOT EXISTS idx_fil_re_share_electricity_capacity_2023 ON fil_re_share_electricity_capacity_2023(country);

INSERT INTO fil_re_share_electricity_capacity_2023 (country, value) VALUES
('N/A',43.04),
('AF',64.04),
('AL',96.57),
('DZ',2.12),
('AS',9.34),
('AD',97.02),
('AO',63.44),
('AI',6.08),
('AG',13.1),
('AR',35.26),
('AM',44.1),
('AW',17.18),
('AU',51.5),
('AT',84.97),
('AZ',20.28),
('BS',1.64),
('BH',0.6),
('BD',3.6),
('BB',19.16),
('BY',5.48),
('BE',54.56),
('BZ',50.01),
('BJ',10.12),
('BQ',33.91),
('BT',99.65),
('BO',32.65),
('BA',44.45),
('BW',0.68),
('VG',5.01),
('BR',85.71),
('BN',0.41),
('BG',47.58),
('BF',35.18),
('BI',60.13),
('CV',25.1),
('KH',58.01),
('CM',55.5),
('CA',69.83),
('KY',7.48),
('CF',38.03),
('TD',1.65),
('CL',63.89),
('CN',49.77),
('HK',2.32),
('TW-TPE',27.5),
('CO',69.54),
('KM',20.24),
('CD',97.69),
('CG',27.54),
('CK',23.29),
('CR',89.35),
('CI','37'),
('HR',72.51),
('CU',11.97),
('CW',22.67),
('CY',34.29),
('CZ',22.71),
('DK',70.57),
('DJ',39.52),
('DM',27.57),
('DO',33.94),
('EC',61.2),
('EG',11.17),
('SV',60.3),
('GQ',31.7),
('ER',11.06),
('EE',53.94),
('SZ',94.74),
('ET',98.24),
('FK',19.91),
('FO',35.89),
('FJ',56.78),
('FI',59.46),
('GF',46.75),
('PF',32.1),
('FR',45.23),
('GA',54.32),
('GM',1.8),
('GE',74.78),
('DE',63.28),
('GH',31.04),
('GR',60.64),
('GL',49.07),
('GD',6.45),
('GP',39.27),
('GU',20.01),
('GT',70.73),
('GN',65.99),
('GW',6.06),
('GY',14.3),
('HT',16.98),
('HN',63.55),
('HU',47.13),
('IS',95.8),
('IN',35.16),
('ID',14.62),
('IR',13.54),
('IQ',5.03),
('IE',48.78),
('IL',20.49),
('IT',51.43),
('JM',16.45),
('JP',34.84),
('JO',37.08),
('KZ',21.88),
('KE',80.05),
('KI',32.94),
('KP',59.63),
('KR',20.19),
('XK',17.16),
('KW',0.56),
('KG',77.65),
('LA',85.74),
('LV',67.73),
('LB',31.8),
('LS',99.41),
('LR',49.6),
('LY',0.07),
('LT',52.51),
('LU',36.03),
('MG',28.14),
('MW',79.73),
('MY',22.81),
('MV',6.82),
('ML',53.78),
('MT',31.96),
('MH',6.02),
('MQ',26.91),
('MR',33.98),
('MU',29.68),
('YT',23.98),
('MX',28.08),
('FM',10.27),
('MD',9.47),
('MN',17.96),
('ME',79.2),
('MS',12.79),
('MA',36.5),
('MZ',78.18),
('MM',49.06),
('NA',74.16),
('NR',11.77),
('NP',98.13),
('NL',59.14),
('NC',24.21),
('NZ',80.7),
('NI',45.93),
('NE',15.13),
('NG',20.88),
('NU',31.11),
('MK',50.76),
('NO',98.21),
('OM',6.23),
('PK',30.59),
('PW',39.43),
('PS',90.57),
('PA',66.71),
('PG',36.05),
('PY',99.71),
('PE',43.15),
('PH',26.94),
('PL',44.18),
('PT',77.56),
('PR',17.13),
('QA',7.22),
('RE',51.09),
('RO',62.23),
('RU',20.74),
('RW',50.36),
('WS',38.48),
('ST',6.39),
('SA',3.26),
('SN',29.02),
('RS',36.83),
('SC',17.85),
('SL',45.28),
('SG',8.8),
('SK',33.32),
('SI',47.77),
('SB',9.74),
('SO',31.81),
('ZA',17.04),
('GS',60.49),
('SS',7.52),
('ES',62.21),
('LK',62.93),
('BL',0.16),
('KN',6.56),
('LC',4.76),
('MF',3.58),
('VC',18.33),
('SD',49.16),
('SR',32.66),
('SE',78.71),
('CH',83.45),
('SY',15.29),
('TJ',88.92),
('TZ',34.4),
('TH',21.37),
('TL',0.23),
('TG',37.71),
('TK',88),
('TO',48.37),
('TT',0.2),
('TN',11.86),
('TR',55),
('TM',0.03),
('TC',3.74),
('TV',54.98),
('UG',92.37),
('UK',51.7),
('UA',24.79),
('AE',13.8),
('UY',77.64),
('VI',2.97),
('US',31.53),
('UZ',14.91),
('VU',34.48),
('VE',47.53),
('VN',55.57),
('YE',14.25),
('ZM',86.42),
('ZW',39.85);

