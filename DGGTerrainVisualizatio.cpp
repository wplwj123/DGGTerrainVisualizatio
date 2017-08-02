// DGGTerrainVisualizatio.cpp : 定义控制台应用程序的入口点。
//

#include <iostream>
#include <string>
#include <vector>

#include <osg/PagedLOD>
#include <osgViewer/Viewer>
#include <osgViewer/config/SingleScreen>
#include <osgViewer/ViewerEventHandlers>
#include <osgGA/StateSetManipulator>
#include <osgUtil/Optimizer>
#include <osgEarth/MapNode>
#include <osgEarthUtil/EarthManipulator>
#include <osgEarthUtil/AutoClipPlaneHandler>

#include <winsock2.h> 
#include <mongo/client/dbclient.h> 
#include <hiredis.h>

#include "LibEQCommon/EQType.h"
#include "LibEQCommon/EQCodec.h"
#include "LibEQUtil/TypeUtil.h"
#include "LibEQUtil/GeoUtil.h"

#if _DEBUG
#pragma comment(lib, "libboost_system-vc120-mt-gd-1_64")
#pragma comment(lib, "libboost_chrono-vc120-mt-gd-1_64")
#pragma comment(lib, "libboost_date_time-vc120-mt-gd-1_64")
#pragma comment(lib, "libboost_thread-vc120-mt-gd-1_64")
#pragma comment(lib, "mongoclient-gd")
#pragma comment(lib, "osgd.lib")
#pragma comment(lib, "osgDBd.lib")
#pragma comment(lib, "osgGAd.lib")
#pragma comment(lib, "osgViewerd.lib")
#pragma comment(lib, "osgUtild.lib")
#pragma comment(lib, "osgEarthd.lib")
#pragma comment(lib, "osgEarthUtild.lib")
#pragma comment(lib, "LibEQCommond.lib")
#pragma comment(lib, "LibEQUtild.lib")
#pragma comment(lib, "hiredisd.lib")
#pragma comment(lib, "Win32_Interopd.lib")
#else
#pragma comment(lib, "libboost_system-vc120-mt-1_64")
#pragma comment(lib, "libboost_chrono-vc120-mt-1_64")
#pragma comment(lib, "libboost_date_time-vc120-mt-1_64")
#pragma comment(lib, "libboost_thread-vc120-mt-1_64")
#pragma comment(lib, "mongoclient")
#pragma comment(lib, "osg.lib")
#pragma comment(lib, "osgDB.lib")
#pragma comment(lib, "osgGA.lib")
#pragma comment(lib, "osgViewer.lib")
#pragma comment(lib, "osgUtil.lib")
#pragma comment(lib, "osgEarth.lib")
#pragma comment(lib, "osgEarthUtil.lib")
#pragma comment(lib, "LibEQCommon.lib")
#pragma comment(lib, "LibEQUtil.lib")
#pragma comment(lib, "hiredis.lib")
#pragma comment(lib, "Win32_Interop.lib")
#endif

mongo::DBClientConnection mongoConn;
const std::string dbName = "Pyramid_test";
redisContext* redisConn;
redisReply* reply;

unsigned int minLevel = 0;
unsigned int maxLevel = 5;
const float viewDistance = 10000000;

const osgEarth::SpatialReference* wgs84 = osgEarth::SpatialReference::get("wgs84", "egm96")->getGeographicSRS();
const osgEarth::SpatialReference* ecef = wgs84->getECEF();

osg::Vec3 SpCoord2ECEF(const eqtm::SphericCoord& sc){

	osgEarth::GeoPoint p(wgs84, sc.longitude, sc.latitude, osgEarth::ALTMODE_ABSOLUTE);
	osgEarth::GeoPoint pc = p.transform(ecef);

	return osg::Vec3(0.8 * pc.x(), 0.8 * pc.y(), 0.8 * pc.z());
}

class NodeBuf : public std::basic_streambuf<char, std::char_traits<char>> {

public:
	//构造函数，参数意义：buffer的地址和长度
	NodeBuf(char* mem, int sz) {
		//参数意义：起始位置，当前位置，最后的位置
		setg(mem, mem, mem + sz);
	}
};

class TileReadFileCallBack : public osgDB::ReadFileCallback{

public:
	virtual osgDB::ReaderWriter::ReadResult readNode(const std::string &fileName, const osgDB::ReaderWriter::Options *options){			 

		int firstIndex = fileName.find_first_of("-");
		int lastIndex = fileName.find_last_of("-");

		if (firstIndex == lastIndex){
			return nullptr;
		}

		reply = (redisReply*)redisCommand(redisConn, "GET %s", fileName.c_str());
		
		std::string contStr = "";
		int size = 0;

		if (reply->type == REDIS_REPLY_STRING){     //get tile from Redis
			contStr = std::string(reply->str, reply->len);
			size = reply->len;
			freeReplyObject(reply);
		}
		else if (reply->type == REDIS_REPLY_NIL){   //get tile from MongoDB
			freeReplyObject(reply);
			
			std::stringstream ss;
			ss << fileName.substr(0, firstIndex);
			unsigned int level;
			ss >> level;

			ss.str("");
			ss.clear();
			ss << fileName.substr(firstIndex + 1, lastIndex);
			unsigned int domID;
			ss >> domID;

			ss.str("");
			ss.clear();
			ss << fileName.substr(lastIndex + 1);
			unsigned int morID;
			ss >> morID;

			unsigned int smallLevel = level % 3;
			unsigned int pyramidLevel = level - smallLevel;

			ss.str("");
			ss.clear();
			ss << dbName << ".Level" << pyramidLevel;
			std::string collName = ss.str();

			mongo::BSONObj topTile = mongoConn.findOne(collName, mongo::BSONObjBuilder().append("DomainID", domID).append("MortonCode", (morID >> (2 * smallLevel))).obj());

			char* content = nullptr;
			if (topTile.isValid()){
				if (smallLevel == 0){      //top level
					content = const_cast<char*>(topTile.getField("Content").binData(size));
				}
				else if (smallLevel == 1){        //middle level
					unsigned int midIndex = morID & 3;
					mongo::BSONElement midTile = topTile.getField("SubPyramidTile").Array().at(midIndex);

					content = const_cast<char*>(midTile.Obj().getField("Content").binData(size));
				}
				else{                      //buttom level
					unsigned int midIndex = (morID & 12) >> 2;
					mongo::BSONElement midTile = topTile.getField("SubPyramidTile").Array().at(midIndex);
					unsigned int btmIndex = (morID & 3);
					mongo::BSONElement btmTile = midTile.Obj().getField("SubPyramidTile").Array().at(btmIndex);

					content = const_cast<char*>(btmTile.Obj().getField("Content").binData(size));
				}
			}

			if (content != nullptr && size != 0){
				reply = (redisReply*)redisCommand(redisConn, "SET %s %b", fileName.c_str(), content, size);
			}
			freeReplyObject(reply);

			contStr = std::string(content, size);

		}
		else{
			std::cout << "search " << fileName << " faile:" << redisConn->errstr << std::endl;
			freeReplyObject(reply);
			return nullptr;
		}	

		char* content = const_cast<char*>(contStr.c_str());
		NodeBuf buffer(content, size);
		std::istream stream(&buffer);

		osg::ref_ptr<osgDB::ReaderWriter> readWriter = osgDB::Registry::instance()->getReaderWriterForExtension("ive");
		osgDB::ReaderWriter::ReadResult result = readWriter->readNode(stream);

		return result;

	}
};

int main(int argc, char* argv[])
{
	mongo::client::initialize();
	
	try {
		mongoConn.connect("127.0.0.1:27017");
		std::cout << "Connect to MongoDB Server Success" << std::endl;
	}
	catch (const mongo::DBException &e) {
		std::cout << "caught " << e.what() << std::endl;
		return -1;
	}

	redisConn = redisConnect("127.0.0.1", 6379);
	if (redisConn->err)
	{
		std::cout << "Connect to Redis Server faile:" << redisConn->errstr << std::endl;
		redisFree(redisConn);
	}
	std::cout << "Connect to Redis Server Success" << std::endl;

	osg::ref_ptr<osgViewer::Viewer> viewer = new osgViewer::Viewer;
	viewer->apply(new osgViewer::SingleScreen(0));
	viewer->getCamera()->setClearColor(osg::Vec4(0, 0, 0, 1));

	osg::Group* root = new osg::Group;

	osg::ref_ptr<osgEarth::Map> map = new osgEarth::Map;
	osg::ref_ptr<osgEarth::MapNode> mapNode = new osgEarth::MapNode(map);
	root->addChild(mapNode);

	osg::ref_ptr<osg::Group> dggNode = new osg::Group;
	root->addChild(dggNode);


	//
	osgDB::Registry::instance()->setReadFileCallback(new TileReadFileCallBack());

	for (unsigned int level = minLevel; level <= maxLevel; level++){
		for (unsigned int dom_id = 0; dom_id < 10; dom_id++){
			eqtm::EQCode areaCode;
			areaCode.dt = 0x04 + static_cast<int>(dom_id << 4);
			areaCode.len = level;
			areaCode.morton = 0;

			EQ_ULLONG n_cell_in_domain = (1 << level) * (1 << level);

			for (EQ_ULLONG mor_id = 0; mor_id < n_cell_in_domain; ++mor_id){
				areaCode.morton = mor_id;

				EQ_UINT code_domain = 0;
				EQ_UINT code_level = 0;
				eqtm::Trigon tri = eqtm::decode(areaCode, code_domain, code_level);
				eqtm::CartesianCoord v1 = eqtm::util::spheric_to_cartesian(tri.v(1));
				eqtm::CartesianCoord v3 = eqtm::util::spheric_to_cartesian(tri.v(2));
				eqtm::SphericCoord center = eqtm::util::cartesian_to_spheric(eqtm::util::mid_great_arc(v1, v3));

				std::stringstream ss;
				ss << level << "-" << dom_id << "-" << mor_id;
				std::string fileName = ss.str();

				osg::ref_ptr<osg::PagedLOD> pageNode = new osg::PagedLOD;
				pageNode->setCenter(SpCoord2ECEF(center));
				pageNode->setFileName(0, fileName);

				if (level == maxLevel){
					pageNode->setRange(0, FLT_MIN, viewDistance / pow(2, level - 1));
				}
				else if (level == minLevel){
					pageNode->setRange(0, viewDistance / pow(2, level), FLT_MAX);
				}
				else{
					pageNode->setRange(0, viewDistance / pow(2, level), viewDistance / pow(2, level - 1));
				}

				dggNode->addChild(pageNode);
			}
		}
	}

	viewer->setUpViewInWindow(50, 50, 1400, 800);

	viewer->addEventHandler(new osgGA::StateSetManipulator(viewer->getCamera()->getOrCreateStateSet()));
	viewer->addEventHandler(new osgViewer::LODScaleHandler);
	viewer->addEventHandler(new osgViewer::ThreadingHandler);
	viewer->addEventHandler(new osgViewer::WindowSizeHandler);
	viewer->addEventHandler(new osgViewer::StatsHandler);
	viewer->addEventHandler(new osgViewer::ScreenCaptureHandler);

	osgUtil::Optimizer optimizer;
	optimizer.optimize(root);

	viewer->setSceneData(root);
	viewer->realize();

	osg::ref_ptr<osgEarth::Util::EarthManipulator> manip = new osgEarth::Util::EarthManipulator();
	manip->getSettings()->setArcViewpointTransitions(true);
	manip->setHomeViewpoint(osgEarth::Viewpoint("World", 110.0, 30.0, 0.0, 0.0, -90.0, (10e6) * 2));
	viewer->setCameraManipulator(manip);

	viewer->run();

	return 0;
}

