Data = LOAD '/usr/local/hadoop/Project/Pig.txt' USING PigStorage(',') AS(SNo:INT,Company:chararray,ReviwerType:chararray,job_title:chararray,overall_ratings:INT,workLifebalance_ratings:INT,culturevalues_ratings:INT,carreropportunities_ratings:INT,benefits_ratings:INT,seniormangemnet_ratings:INT);

data = FOREACH Data GENERATE SNo,Company,ReviwerType,TRIM(job_title),overall_ratings,workLifebalance_ratings,culturevalues_ratings,carreropportunities_ratings,benefits_ratings,seniormangemnet_ratings;

f_Data_RType_E = FILTER Data BY ReviwerType == 'Employee';

f_Data_RType_E_BY_G = FILTER f_Data_RType_E BY Company == 'google';
f_Data_RType_E_BY_Am = FILTER f_Data_RType_E BY Company == 'amazon';
f_Data_RType_E_BY_FB = FILTER f_Data_RType_E BY Company == 'facebook';
f_Data_RType_E_BY_NT = FILTER f_Data_RType_E BY Company == 'netflix';
f_Data_RType_E_BY_AP = FILTER f_Data_RType_E BY Company == 'apple';
f_Data_RType_E_BY_MS = FILTER f_Data_RType_E BY Company == 'microsoft';


Group_Avg_ratings_G_E = Group f_Data_RType_E_BY_G All;
dis_G_E = DISTINCT(FOREACH f_Data_RType_E_BY_G GENERATE $1, $2);
All_ratings_G_E = FOREACH Group_Avg_ratings_G_E  Generate (dis_G_E.$0),(dis_G_E.$1), ROUND(AVG(f_Data_RType_E_BY_G.overall_ratings)),ROUND(AVG(f_Data_RType_E_BY_G.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_E_BY_G.culturevalues_ratings)),ROUND(AVG(f_Data_RType_E_BY_G.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_E_BY_G.benefits_ratings)),ROUND(AVG(f_Data_RType_E_BY_G.seniormangemnet_ratings));


Group_Avg_ratings_A_E = Group f_Data_RType_E_BY_Am All;
dis_A_E = DISTINCT(FOREACH f_Data_RType_E_BY_Am GENERATE $1, $2);
All_ratings_A_E = foreach Group_Avg_ratings_A_E Generate (dis_A_E.$0),(dis_A_E.$1), ROUND(AVG(f_Data_RType_E_BY_Am.overall_ratings)),ROUND(AVG(f_Data_RType_E_BY_Am.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_E_BY_Am.culturevalues_ratings)),ROUND(AVG(f_Data_RType_E_BY_Am.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_E_BY_Am.benefits_ratings)),ROUND(AVG(f_Data_RType_E_BY_Am.seniormangemnet_ratings));


Group_Avg_ratings_FB_E = Group f_Data_RType_E_BY_FB All;
dis_FB_E = DISTINCT(FOREACH f_Data_RType_E_BY_FB GENERATE $1, $2);
All_ratings_FB_E = foreach Group_Avg_ratings_FB_E Generate (dis_FB_E.$0),(dis_FB_E.$1), ROUND(AVG(f_Data_RType_E_BY_FB.overall_ratings)),ROUND(AVG(f_Data_RType_E_BY_FB.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_E_BY_FB.culturevalues_ratings)),ROUND(AVG(f_Data_RType_E_BY_FB.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_E_BY_FB.benefits_ratings)),ROUND(AVG(f_Data_RType_E_BY_FB.seniormangemnet_ratings));

Group_Avg_ratings_NT_E = Group f_Data_RType_E_BY_NT All;
dis_NT_E = DISTINCT(FOREACH f_Data_RType_E_BY_NT GENERATE $1, $2);
All_ratings_NT_E = foreach Group_Avg_ratings_NT_E Generate (dis_NT_E.$0),(dis_NT_E.$1), ROUND(AVG(f_Data_RType_E_BY_NT.overall_ratings)),ROUND(AVG(f_Data_RType_E_BY_NT.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_E_BY_NT.culturevalues_ratings)),ROUND(AVG(f_Data_RType_E_BY_NT.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_E_BY_NT.benefits_ratings)),ROUND(AVG(f_Data_RType_E_BY_NT.seniormangemnet_ratings));


Group_Avg_ratings_Ap_E = Group f_Data_RType_E_BY_AP All;
dis_AP_E = DISTINCT(FOREACH f_Data_RType_E_BY_AP GENERATE $1, $2);
All_ratings_Ap_E = foreach Group_Avg_ratings_Ap_E Generate (dis_AP_E.$0),(dis_AP_E.$1), ROUND(AVG(f_Data_RType_E_BY_AP.overall_ratings)),ROUND(AVG(f_Data_RType_E_BY_AP.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_E_BY_AP.culturevalues_ratings)),ROUND(AVG(f_Data_RType_E_BY_AP.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_E_BY_AP.benefits_ratings)),ROUND(AVG(f_Data_RType_E_BY_AP.seniormangemnet_ratings));


Group_Avg_ratings_MS_E = Group f_Data_RType_E_BY_MS All;
dis_Ms_E = DISTINCT(FOREACH f_Data_RType_E_BY_MS GENERATE $1, $2);
All_ratings_MS_E = foreach Group_Avg_ratings_MS_E Generate (dis_Ms_E.$0),(dis_Ms_E.$1),  ROUND(AVG(f_Data_RType_E_BY_MS.overall_ratings)),ROUND(AVG(f_Data_RType_E_BY_MS.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_E_BY_MS.culturevalues_ratings)),ROUND(AVG(f_Data_RType_E_BY_MS.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_E_BY_MS.benefits_ratings)),ROUND(AVG(f_Data_RType_E_BY_MS.seniormangemnet_ratings));


f_Data_RType_AL = FILTER Data BY ReviwerType == 'Alumni';

f_Data_RType_AL_BY_G = FILTER f_Data_RType_AL BY Company == 'google';
f_Data_RType_AL_BY_Am = FILTER f_Data_RType_AL BY Company == 'amazon';
f_Data_RType_AL_BY_FB = FILTER f_Data_RType_AL BY Company == 'facebook';
f_Data_RType_AL_BY_NT = FILTER f_Data_RType_AL BY Company == 'netflix';
f_Data_RType_AL_BY_AP = FILTER f_Data_RType_AL BY Company == 'apple';
f_Data_RType_AL_BY_MS = FILTER f_Data_RType_AL BY Company == 'microsoft';


Group_Avg_ratings_G_AL = Group f_Data_RType_AL_BY_G All;
dis_G_Al = DISTINCT(FOREACH f_Data_RType_AL_BY_G GENERATE $1, $2);
All_ratings_G_Al = foreach Group_Avg_ratings_G_AL Generate (dis_G_Al.$0),(dis_G_Al.$1), ROUND(AVG(f_Data_RType_AL_BY_G.overall_ratings)),ROUND(AVG(f_Data_RType_AL_BY_G.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_AL_BY_G.culturevalues_ratings)),ROUND(AVG(f_Data_RType_AL_BY_G.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_AL_BY_G.benefits_ratings)),ROUND(AVG(f_Data_RType_AL_BY_G.seniormangemnet_ratings));


Group_Avg_ratings_A_AL = Group f_Data_RType_AL_BY_Am All;
dis_A_Al = DISTINCT(FOREACH f_Data_RType_AL_BY_Am GENERATE $1, $2);
All_ratings_A_Al = foreach Group_Avg_ratings_A_AL Generate (dis_A_Al.$0),(dis_A_Al.$1), ROUND(AVG(f_Data_RType_AL_BY_Am.overall_ratings)),ROUND(AVG(f_Data_RType_AL_BY_Am.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_AL_BY_Am.culturevalues_ratings)),ROUND(AVG(f_Data_RType_AL_BY_Am.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_AL_BY_Am.benefits_ratings)),ROUND(AVG(f_Data_RType_AL_BY_Am.seniormangemnet_ratings));


Group_Avg_ratings_FB_AL = Group f_Data_RType_AL_BY_FB All;
dis_FB_Al = DISTINCT(FOREACH f_Data_RType_AL_BY_FB GENERATE $1, $2);
All_ratings_FB_AL = foreach Group_Avg_ratings_FB_AL Generate (dis_FB_Al.$0),(dis_FB_Al.$1), ROUND(AVG(f_Data_RType_AL_BY_FB.overall_ratings)),ROUND(AVG(f_Data_RType_AL_BY_FB.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_AL_BY_FB.culturevalues_ratings)),ROUND(AVG(f_Data_RType_AL_BY_FB.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_AL_BY_FB.benefits_ratings)),ROUND(AVG(f_Data_RType_AL_BY_FB.seniormangemnet_ratings));


Group_Avg_ratings_NT_AL = Group f_Data_RType_AL_BY_NT All;
dis_NT_Al = DISTINCT(FOREACH f_Data_RType_AL_BY_NT GENERATE $1, $2);
All_ratings_NT_Al = foreach Group_Avg_ratings_NT_AL Generate (dis_NT_Al.$0),(dis_NT_Al.$1), ROUND(AVG(f_Data_RType_AL_BY_NT.overall_ratings)),ROUND(AVG(f_Data_RType_AL_BY_NT.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_AL_BY_NT.culturevalues_ratings)),ROUND(AVG(f_Data_RType_AL_BY_NT.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_AL_BY_NT.benefits_ratings)),ROUND(AVG(f_Data_RType_AL_BY_NT.seniormangemnet_ratings));


Group_Avg_ratings_Ap_AL = Group f_Data_RType_AL_BY_AP All;
dis_AP_Al = DISTINCT(FOREACH f_Data_RType_AL_BY_AP GENERATE $1, $2);
All_ratings_Ap_Al = foreach Group_Avg_ratings_Ap_AL Generate (dis_AP_Al.$0),(dis_AP_Al.$1),  ROUND(AVG(f_Data_RType_AL_BY_AP.overall_ratings)),ROUND(AVG(f_Data_RType_AL_BY_AP.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_AL_BY_AP.culturevalues_ratings)),ROUND(AVG(f_Data_RType_AL_BY_AP.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_AL_BY_AP.benefits_ratings)),ROUND(AVG(f_Data_RType_AL_BY_AP.seniormangemnet_ratings));

Group_Avg_ratings_MS_AL = Group f_Data_RType_AL_BY_MS All;
dis_Ms_Al = DISTINCT(FOREACH f_Data_RType_AL_BY_MS GENERATE $1, $2);
All_ratings_Ms_Al = foreach Group_Avg_ratings_MS_AL Generate (dis_Ms_Al.$0),(dis_Ms_Al.$1), ROUND(AVG(f_Data_RType_AL_BY_MS.overall_ratings)),ROUND(AVG(f_Data_RType_AL_BY_MS.workLifebalance_ratings)),ROUND(AVG(f_Data_RType_AL_BY_MS.culturevalues_ratings)),ROUND(AVG(f_Data_RType_AL_BY_MS.carreropportunities_ratings)),ROUND(AVG(f_Data_RType_AL_BY_MS.benefits_ratings)),ROUND(AVG(f_Data_RType_AL_BY_MS.seniormangemnet_ratings));


File = UNION All_ratings_G_E,All_ratings_A_E,All_ratings_FB_E,All_ratings_NT_E,All_ratings_Ap_E,All_ratings_MS_E,All_ratings_G_Al,All_ratings_A_Al,All_ratings_FB_AL,All_ratings_NT_Al,All_ratings_Ap_Al,All_ratings_Ms_Al;

STORE data INTO '/usr/local/hadoop/Project/HIVE' using PigStorage(',');
STORE File INTO '/usr/local/hadoop/Project/Test' using PigStorage(',');
