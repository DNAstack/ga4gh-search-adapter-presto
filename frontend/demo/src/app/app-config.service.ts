import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {environment} from '../environments/environment';

@Injectable()
export class AppConfigService {
  config;

  constructor(private http: HttpClient) {
  }

  loadAppConfig() {
    return this.http.get('/assets/appConfig.json')
      .toPromise()
      .then(data => {
        this.config = Object.assign({}, environment, data);
        console.debug('Runtime configuration', this.config);
      });
  }
}
