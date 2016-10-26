#include <iostream>
#include <string>
#include <vector>
#include <RTreeLoad.h>


class MyDataStream : public IDataStream
{
public:
	MyDataStream(std::string inputFile) : m_pNext(0)
	{
		m_filename = inputFile;

		m_id=0;
		m_fin.open(inputFile.c_str());

		if (! m_fin)
			throw Tools::IllegalArgumentException("Input file not found.");

		readNextEntry();
	}

	virtual ~MyDataStream()
	{
		if (m_pNext != 0) delete m_pNext;
	}

	virtual IData* getNext()
	{
		if (m_pNext == 0) return 0;

		RTree::Data* ret = m_pNext;
		m_pNext = 0;
		readNextEntry();
		return ret;
	}

	virtual bool hasNext()
	{
		return (m_pNext != 0);
	}

	virtual uint32_t size()
	{
		throw Tools::NotSupportedException("Operation not supported.");
	}

	virtual void rewind()
	{
		if (m_pNext != 0)
		{
			delete m_pNext;
			m_pNext = 0;
		}

		m_fin.seekg(0, std::ios::beg);
		readNextEntry();
	}

	void readNextEntry()
	{
		uint32_t op;
		std::string line;

		if (m_fin.good())
		{
			getline(m_fin,line);
			if (line.length()==0) return;
			m_pNext = parseInputLine(line, m_filename, m_id);
		}
	}

	std::ifstream m_fin;
	string m_filename;
	RTree::Data* m_pNext;
	id_type m_id;
};
