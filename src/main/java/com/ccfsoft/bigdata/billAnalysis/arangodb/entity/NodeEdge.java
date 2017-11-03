/*
 * DISCLAIMER
 *
 * Copyright 2016 ArangoDB GmbH, Cologne, Germany
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright holder is ArangoDB GmbH, Cologne, Germany
 */

package com.ccfsoft.bigdata.billAnalysis.arangodb.entity;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;

/**
 * @author a-brandt
 *
 */
public class NodeEdge {

	@DocumentField(Type.ID)
	private String id;

	@DocumentField(Type.KEY)
	private String key;

	@DocumentField(Type.REV)
	private String revision;

	@DocumentField(Type.FROM)
	private String from;

	@DocumentField(Type.TO)
	private String to;

	/** 时间 **/
	private String dataTime;

	/** 时长 **/
	private int seconds;

	public NodeEdge(final String from, final String to,final String dataTime, int seconds) {
		this.from = from;
		this.to = to;
		this.dataTime = dataTime;
		this.seconds = seconds;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getRevision() {
		return revision;
	}

	public void setRevision(String revision) {
		this.revision = revision;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getDataTime() {
		return dataTime;
	}

	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}

	public int getSeconds() {
		return seconds;
	}

	public void setSeconds(int seconds) {
		this.seconds = seconds;
	}
}
