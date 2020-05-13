/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloudfoundry

import (
	"github.com/cloudfoundry-community/go-cfclient"
	"time"
)

// AppInfoRepository represents a Cloud Foundry application's information.
type AppInfoRepository interface {

	// GetAppInfo gets the basic information for a CF application.
	GetAppInfo(string) AppInfo
}

// AppInfo is the basic information for a CF application.
type AppInfo struct {
	AppName     string
	SpaceGUID   string
	SpaceName   string
	OrgGUID     string
	OrgName     string
	LastQueried time.Time
}

// NewAppInfoRepository creates a new AppInfoRepository given a CF client.
func NewAppInfoRepository(cfClient *cfclient.Client, appMetadataCachePeriod int) AppInfoRepository {
	return &appInfoRepository{cfClient, map[string]AppInfo{}, appMetadataCachePeriod}
}

// NullAppInfoRepository creates a new AppInfoRepository with Go default values.
func NullAppInfoRepository() AppInfoRepository {
	return &nullAppInfoRepository{}
}

type appInfoRepository struct {
	cfClient               *cfclient.Client
	cache                  map[string]AppInfo
	appMetadataCachePeriod int
}

func (air *appInfoRepository) GetAppInfo(guid string) AppInfo {
	// Handle cacheable configurations
	if air.appMetadataCachePeriod != 0 {
		appInfo, ok := air.cache[guid]

		if ok {
			if air.appMetadataCachePeriod > 0 {
				metadataReadTime := appInfo.LastQueried
				// elapsedTime is in seconds, time.Since returns a duration, so we need to convert to seconds
				elapsedTime := time.Since(metadataReadTime).Seconds()

				if elapsedTime < float64(air.appMetadataCachePeriod) {
					return appInfo
				}
			} else {
				return appInfo
			}
		}
	}

	return air.QueryCfForMetadata(guid)
}

func (air *appInfoRepository) QueryCfForMetadata(guid string) AppInfo {
	var appInfo AppInfo
	app, err := air.cfClient.AppByGuid(guid)
	if err == nil {
		appInfo := AppInfo{
			AppName:     app.Name,
			SpaceGUID:   app.SpaceData.Entity.Guid,
			SpaceName:   app.SpaceData.Entity.Name,
			OrgGUID:     app.SpaceData.Entity.OrgData.Entity.Guid,
			OrgName:     app.SpaceData.Entity.OrgData.Entity.Name,
			LastQueried: time.Now(),
		}
		air.cache[guid] = appInfo
	}
	return appInfo
}

type nullAppInfoRepository struct{}

func (nair *nullAppInfoRepository) GetAppInfo(guid string) AppInfo {
	return AppInfo{}
}
