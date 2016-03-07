package com.analysis.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CreateData {
	public static void main(String args[]) throws IOException, ParseException{
		//创建我们的数据
		//第一列数据为通过关卡的ID号，我们要创建30天的数据,创建20150301到20150331的数据
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/home/grid/data/bigdata.txt")));
		Map<Integer,String> map = new HashMap<Integer,String>();
		map.put(0, "伊力牙尔");map.put(1,"艾木热江");map.put(2, "吐尔地");map.put(3, "阿布都比拉力");map.put(4, "卡地");map.put(5,"张");
		map.put(6,"美门江");map.put(7, "李");map.put(8,"艾斯卡尔");map.put(9, "古丽尼沙");map.put(10, "拜力克孜");
		//民族
		Map<Integer,String> nationmap = new HashMap<Integer,String>();
		nationmap.put(0,"柯尔克孜族");nationmap.put(1,"维吾尔族");nationmap.put(2,"汉族");nationmap.put(3,"哈萨克族");nationmap.put(4,"回族");nationmap.put(5,"柯尔克孜族");nationmap.put(6,"蒙古族");nationmap.put(7,"塔吉克族");
		nationmap.put(8,"满族");nationmap.put(9,"乌孜别克族");nationmap.put(10,"俄罗斯族");nationmap.put(11,"达斡尔族");nationmap.put(12,"塔塔尔族");nationmap.put(13,"维吾尔族");nationmap.put(14,"维吾尔族");
		nationmap.put(15,"维吾尔族");nationmap.put(16,"维吾尔族");nationmap.put(17,"维吾尔族");nationmap.put(18,"汉族");nationmap.put(19,"汉族");nationmap.put(20,"汉族");nationmap.put(21,"汉族");
		nationmap.put(22,"哈萨克族");nationmap.put(23,"哈萨克族");nationmap.put(24,"回族");nationmap.put(25,"回族");
		//地址
		Map<Integer,String> addressMap = new HashMap<Integer,String>();
		addressMap.put(0,"甘肃天水");addressMap.put(1,"新疆察布查尔锡伯自治县");  addressMap.put(2,"新疆和布克赛尔蒙古自治县");   addressMap.put(3,"新疆木垒哈萨克自治县"); addressMap.put(4,"新疆和田县"); 
		addressMap.put(5,"四川青羊县"); addressMap.put(6,"四川大安区 ");addressMap.put(7,"内蒙古根河市林海南路 ");addressMap.put(7,"吉林省四平市铁东区 ");addressMap.put(7,"吉林省四平市铁东区 ");addressMap.put(8,",河南省新郑市观音寺镇");		    
		//关卡地点	 
		 Map<Integer,String> EntryPositionMap = new HashMap<Integer, String>();
		 EntryPositionMap.put(0,"650100005");EntryPositionMap.put(1,"650100001");EntryPositionMap.put(2,"650100002");EntryPositionMap.put(3,"650100003");EntryPositionMap.put(4,"650100004");EntryPositionMap.put(5,"650100005");EntryPositionMap.put(6,"650100006");EntryPositionMap.put(7,"650100007");EntryPositionMap.put(8,"650100008");EntryPositionMap.put(9,"650100009");EntryPositionMap.put(10,"650100001");EntryPositionMap.put(11,"650100001");EntryPositionMap.put(12,"650100005");EntryPositionMap.put(13,"650100005");EntryPositionMap.put(14,"650100005");EntryPositionMap.put(15,"650100005");EntryPositionMap.put(16,"650100010");		 
		 //事件代码	 
		 Map<Integer,String> codeMap = new HashMap<Integer, String>();
		 codeMap.put(0,"10002");codeMap.put(1,"10002");codeMap.put(2,"10002");codeMap.put(3,"NULL");codeMap.put(4,"NULL");codeMap.put(5,"NULL");codeMap.put(6,"NULL");codeMap.put(7,"NULL");codeMap.put(8,"NULL");codeMap.put(9,"NULL"); 
		 //监控地点
        Map<Integer,String> Devicemap = new HashMap<Integer, String>();
        Devicemap.put(0, "乌拉泊公安检查站入城1"); Devicemap.put(1, "乌拉泊公安检查站入城2"); Devicemap.put(2, "乌拉泊公安检查站入城3");Devicemap.put(3, "乌拉泊公安检查站入城4");
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date startDate = dateFormat.parse("20150301000000");
		
		Random rand = new Random();
		for(int add = 0 ; add < 30 ; add++){	
			Calendar c = Calendar.getInstance();
			c.setTime(startDate);
			c.add(Calendar.DATE, add);
			//随机一个数是每天产生的数据，从100000~110000
			int nums = 100000+ new Random().nextInt(10000);
			//int nums = 1000 ;
			int temp = 0;
			while(temp<nums){
				String timeStr="";
				int randTime = 0;
				String name = "";
				String sex = "";
				String nation = "";
				int byear; int bmonth ; int bday ;
				String birthday = "";
				String address ="";
				String CardNO = "";
				String SignGov ="";
				String LimitedDate = "";
				String EntryTime = "";
				int Dubious= 0;
				String Photo = "";
				String EntryPosition = "";
				int EntryTodayTimes= 0;
				String  EventCode  = "";
				String DeviceName = "";
				String UploadFlag ="";
				String DeviceIP = "";
				String JingZhongint ;
				String CreateTime ="";
				String Destination = "";
				String Phone = "";
				String Verify  = "";
				String Remark  = "";
				String Direction = "";
				int CheckResult ;
				//开始设计我们的数据，首先设计我们的的日期，我们设计一秒钟过一个人，且产生一个水机数
				if(temp<86400){
					c.add(Calendar.SECOND, 1);
					randTime = 100000+rand.nextInt(899999);
					timeStr=dateFormat.format(c.getTime())+randTime;
				}
				else{
					int randTime1 = 100000+rand.nextInt(899999);
					while(randTime1== randTime){
						randTime1 = 100000+rand.nextInt(899999);
					}
					timeStr=dateFormat.format(c.getTime())+randTime1;
				}
				//下面设计我们的姓名
				int startNameNum = 0 ;
				while(startNameNum<3){
					int key = rand.nextInt(11);
					if(startNameNum==2){
						name += map.get(key);
					}
					else{
						name+=map.get(key)+".";
						
					}
					startNameNum++;
				}	
				//System.out.println(timeStr+","+name);
				temp++;
				//随机一个男女
				boolean sexbo = rand.nextBoolean();
				if(sexbo){
					sex = "男";
				}
				else{
					sex = "女";
				}
			    //随机一个民族
				int nationkey = rand.nextInt(25);
				nation = nationmap.get(nationkey);		
				//随机产生一个生日
				byear = 1930 + rand.nextInt(86);
				String month ="";
				bmonth = 1+rand.nextInt(12);
				if((bmonth/10)!=0){
					month=bmonth+"";
				}
				else{
					month = "0"+bmonth;
				}
				bday = 1 + rand.nextInt(31);
				String day ="";
				bmonth = 1+rand.nextInt(12);
				if((bday/10)!=0){
					day=bday+"";
				}
				else{
					day = "0"+bday;
				}
				
				birthday = byear+"-"+month+"-"+bday+" "+"00:00:00.000";		
				//地址
				address = addressMap.get(rand.nextInt(8));	
				//身份证号
  		        CardNO = ""+(100000+rand.nextInt(899999))+""+byear+""+month+""+day+""+(1000+rand.nextInt(8999));	
  		        
			    //发证地区	    
			    SignGov = address+"公安局";    
			    //身份证有效期限
			    LimitedDate="2011年02月14日--2021年02月14日";    
			    //入卡时间	    
			    EntryTime = dateFormat1.format(c.getTime())+".000";   
			    //可疑人员
			    Dubious = 0;   
			    //Photo只是用一张图片
			    Photo ="0xFFD8FFE000104A46494600010101006000600000FFDB004300080606070605080707070909080A0C140D0C0B0B0C1912130F141D1A1F1E1D1A1C1C20242E2720222C231C1C2837292C30313434341F27393D38323C2E333432FFDB0043010909090C0B0C180D0D1832211C213232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232FFC0001108007E006603012200021101031101FFC4001F0000010501010101010100000000000000000102030405060708090A0BFFC400B5100002010303020403050504040000017D01020300041105122131410613516107227114328191A1082342B1C11552D1F02433627282090A161718191A25262728292A3435363738393A434445464748494A535455565758595A636465666768696A737475767778797A838485868788898A92939495969798999AA2A3A4A5A6A7A8A9AAB2B3B4B5B6B7B8B9BAC2C3C4C5C6C7C8C9CAD2D3D4D5D6D7D8D9DAE1E2E3E4E5E6E7E8E9EAF1F2F3F4F5F6F7F8F9FAFFC4001F0100030101010101010101010000000000000102030405060708090A0BFFC400B51100020102040403040705040400010277000102031104052131061241510761711322328108144291A1B1C109233352F0156272D10A162434E125F11718191A262728292A35363738393A434445464748494A535455565758595A636465666768696A737475767778797A82838485868788898A92939495969798999AA2A3A4A5A6A7A8A9AAB2B3B4B5B6B7B8B9BAC2C3C4C5C6C7C8C9CAD2D3D4D5D6D7D8D9DAE2E3E4E5E6E7E8E9EAF2F3F4F5F6F7F8F9FAFFDA000C03010002110311003F00F7EA28A2800A28A6BCB1C7F7DD57FDE38A00753259A2810BCB22A28EEC702B2F5CF1269FA16992DEDC4E8C107CA8AC0963E82BC03C47E35D4BC4B725AE6529006252153F2AFF0089F7A695C895451D0F73B8F1DF872DA42875289C8EBE5FCC055DD33C4DA3EAE84D9DFC2E47552C011F81AF97DA4C9CE69D14ED1BEE56208EE0D5F2223DA9F5A039191D28AF10F05FC4ABAD3592CB5366B8B52DC48C4974FF00115ED36B750DEDB25C5BC8B244E32ACA7208A86AC691929135145148A0A28A2800A28AA9AA5DFD874BB9BA18CC5196E680396F1978FED7C361ADA1026BCC7DDECB5E25ACF8CF55D5272F25C3A8C9C05635575DD4E6D4B5396799CB3B1CE49EB58CE096AD546C714E72948B336A375709896E24619CE09E2AB893D4D3A2B5797EE2E69EF65228F9908FC295F52B91B57181BD1A9DB8D46602A3914AAA54E08AB23544EB26D5EB8AF4FF0085FE33FB05C2E8F7847D9A76FDDB93F71BD3F1FE67DEBCADB07A678AB16ACC8E194F4349AB97176773EB804100839068AE7BC15AB9D63C336D348FBA641B1CE79C8EE7EB5D0D6275277570A28A2818571BF137527D3FC233226E0D70C23C8F4EFF00A57655E7DF1701FF008462139E04BC8F5E29C77226ED1678048CD23EE3CD4F6F6C653C0EB51AAE3815D0E916A9B559F8C539C9A33A5052DCBDA3693839DB5B173A54122956419F5C54D65736D13052E076E6B51EDFCF5DF13861DB06B9A5396E7A11A71B58F3DD434B5859971D391F4AC194AAB71D6BD1755D36E2650550965C8FC0D713A969525B312C9815BD29DD6A7257A496A8CBDF9249EF5344C00C0E84D4422A95142D6D7395267B4FC1CD40BDB6A1A7EDF9632B286CFAF18FD2BD4ABC43E105C3A7895E1462125B762E3D718C7F335EDF594B73A69FC214514522C2B8DF89B63F6CF084CC33BA160C00EF5D95727F10AF9AD3C38D122066B86F2C67B714D3B038F3687CE502EE9D53DEBB4D3EC09508C302B95B3B6D9AEDBC0FD0B80457A625A663C26413515645D0A7AD8822D0F4F913F78559BD4E322AFD85A2D9CAA216CC7ED59D71A1C93C5B04B246D9CEE51D6B66C2D8DAC214927D33DAB9DBB9DA92BEC684C15A16D8304F7AE7AEB4382F982DCC8DF406BA5560F1802B9CD5EC2F24949B69DE23B872076A4A4D04A09EE674DE0DB089FE4E57D0F35CBF89F43FB046B3DBFCB1F702BBF86D2E414DF348C00C1CF7ACBF18DBFF00C482738FBB823F3AD6155DECCE69D18D9B33FE11DCF95E2D8B7293BE264E3B6715F4057CFBF0AAEA1B2F11ABCEBC38D88D8E84D7D055BC9DD9CD08B51D428A28A450572BE3EB06BCD05645CFEE240E4019E3A575550DDDB25E5A4B6D20CA48A54D0541F2C933E719ED73E2486403953BABD06CB1C5676ABA13699A93ACE80B272AFF00DE1EB53DACF8239AE79BB9D90493BAEA6F80A2AA5CCCAADB4669CB3175E2B2F554BC2B9B56447DC396E78A8B1B7536A15C43B854B118E45C31E6B9FF00B46ABF6211092312F4DDCE2B46C167F250CE57CCC73B7A1A4D033425882A923A572BE2F39F0FDC2AF538AE966B82232335CF6AC82EEDCC07F8CFE94E2B5266AF1B19DE06D2924BAD3518105A50E48EA300D7B95713E0BD04DB95BC923C222E22C8EBEF5DB574C4E3AAD6915D105145154621451450064EBFA443AA69D2ABA8F3154956EE2BCC2252BD4E4D7B21C60E718EF9AF23D4AE6C8EBB790DA4E92AAC873B5B3CF7FD6B2A8BA9B529DB426598C71122AA3EB10AB156662E3B019A9A36561B1BA541369D119B7A28CD64764257DC3FB593EF6C23F0353DBEB11B48123DDB89E8462A34B47CE368AB515AA43F315F9A93653B16A66DF9E29FA269F1EA1AF411CAA1A3442CC08E0FF9CD53926DA09CF02B57C11E24D125BA9ED7ED71ADF33ED557E0B63D3D7BD5C237673D4A965A1DFAAAA28550001D00A5A28AE838C28A28A0028270327A515C8FC41F13A787B41748DBFD2EE54A4401E57D5BF0A695C4DD95CF3FF8A7F106692E9F45D22E8AC11F13CB131059BFBB9F41FF00D6F5AE0FC11231D6A45739061623EB9158B382CCCE49249C926B5FC1AD8D6F3E91B7F4AB9A4A273C24DCEE7A0A4FE5CBB5F35A715CC7B776722B3E58BCD19AAAD05C01B627FC0D71591E9C64746B7B101906A39AEE36CB67F0AE7A2B4D499C29002FAE6B56DF4E31906672E7D3B54BB1A7304CACF6934AC485D8481F85789FDA248AECCB1C8C8E1B706538208AF73BE602CA58C742A47E95E0D38DB73229EC715D18738B11267B77C33F89A66F2B46D726CB9F960B863D7FD963EBEFDFEBD7D90104020E41E86BE33B762922BA9E95F467C30F170D6F485D3EEA406F6D970339CBA7AFB9F5FF00EBD6F38F5473D2A97D19E8145145666E52D5356B2D1AC64BBBE9D628906793C9F603BD7CE7E35F11CDE23D765B9666100F961427EEAD41AF788F51D72EBED57F3191B9DABD1507A01584CE642188C56D18DB5309CF9B411D3721153F86E6FB36B601380548FE5518276D578D8C17EB2AF5CD29EC453F88F60B43E6A807BD4EF6EC92654565E8D74D2DB2498C3639AE891B74618F5AE067A312189A5C60F02AC2C64F279A50E0FF0008A56722338ACD9A195AABAA46FCF635E29AAC3E56A57031C172457AF6AACD296527007EB5E61AE460DEC87EB5D740E2C4EC66C03E4AEAFC19AB49A2EB96F7A198246DF301DD7B8C77AE56DBFD5D695AC850022BA9EF63860CFA6B4EF1B787F528B7A6A504247559DC467F5C515F389BC0BF79739F4A297B3474FB567FFFD9";    
			    //关卡的地点 
			    EntryPosition = EntryPositionMap.get(rand.nextInt(15)); 
			    //进入管卡的次数	    
			    EntryTodayTimes = 1 + rand.nextInt(4);
			    //System.out.println(EntryTodayTimes);       
			    //事件代码？？
			    EventCode = codeMap.get(rand.nextInt(9));
			    //监控设备
			    DeviceName = Devicemap.get(rand.nextInt(3));
			    
			    //NULL,,NULL,2015-01-01 00:04:32.077,NULL,NULL,NULL,NULL,NULL,-1
			    //??
			    UploadFlag = "NULL";
			    //设备IP
			    DeviceIP = "NULL";
			    //??
			    JingZhongint = "NULL";
			    //??
			    Calendar ctmp = Calendar.getInstance();
			    ctmp.setTime(c.getTime());
			    ctmp.add(Calendar.MINUTE, -2);
			    CreateTime = dateFormat2.format(ctmp.getTime());
			    //目的地
			    Destination = "NULL";
			    //手机号
			    Phone = "NULL";
		        //校验
			    Verify = "NULL";	    
			    //方向
			    Direction="NULL";		    
			    //检查结果		    
			    CheckResult = -1;      
			    //将结果最终输入到我们的文件中 
			    String res = timeStr+","+name+","+sex+","+nation+","+birthday+","+address+","+CardNO+","+SignGov+","+LimitedDate+","+EntryTime+","+Dubious+","+Photo+","+EntryPosition+","+EntryTodayTimes+","+EventCode+","+DeviceName+","+UploadFlag+","+DeviceIP+","+JingZhongint+","+CreateTime+","+Destination+","+Phone+","+Verify+","+Remark+","+Direction+","+CheckResult;
			    writer.write(res);
			    writer.newLine();
			    writer.flush();
			    temp++;
			}	
		}	
		writer.close();	
	}
}